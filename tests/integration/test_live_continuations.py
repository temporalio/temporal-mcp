"""Live continuation acceptance through the actual MCP boundary.

This opt-in test owns an isolated Temporal dev server and its fixture processes. Run with
``OPS_MCP_INTEGRATION=1 pytest tests/integration/test_live_continuations.py -q``.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from ops_mcp.acceptance.runner import (
    FixtureOrchestrator,
    build_server_command,
    child_environment,
    fixture_steps,
    reserve_local_port,
    wait_for_port,
    write_target_config,
)
from ops_mcp.contracts import PopulationAdvancedOptions, PopulationLimits
from ops_mcp.mcp import build_composition_root, create_mcp

_TEMPORAL = shutil.which("temporal")

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_INTEGRATION") != "1" or _TEMPORAL is None,
    reason="set OPS_MCP_INTEGRATION=1 and install the Temporal CLI",
)

_HISTORY_TASK_QUEUE = "live-continuation-history"


@workflow.defn
class _PagedHistoryWorkflow:
    @workflow.run
    async def run(self, iterations: int) -> None:
        for _ in range(iterations):
            await workflow.sleep(timedelta(milliseconds=1))


def _small_population_limits(_: PopulationAdvancedOptions) -> PopulationLimits:
    """Force multiple Visibility frames without weakening production defaults."""
    return PopulationLimits(
        max_candidates=2,
        max_visibility_pages=1,
        max_runs=2,
        max_histories=2,
        max_history_events_per_run=100,
        max_task_queues=4,
        max_concurrency=2,
    )


def _large_population_limits(_: PopulationAdvancedOptions) -> PopulationLimits:
    """Traverse large fixtures in bounded frames while limiting exact-run fan-out."""
    return PopulationLimits(
        max_candidates=100,
        max_visibility_pages=1,
        max_runs=4,
        max_histories=4,
        max_history_events_per_run=100,
        max_task_queues=8,
        max_concurrency=4,
    )


async def _call_fresh(config: Path, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct the entire application between continuation pages."""
    root = build_composition_root(config)
    server = create_mcp(root)
    try:
        _, structured = await server.call_tool(tool, arguments)
        assert structured is not None
        return structured
    finally:
        await root.shutdown()


async def _seed_paged_history(port: int) -> tuple[str, str]:
    client = await Client.connect(f"127.0.0.1:{port}")
    worker = Worker(
        client,
        task_queue=_HISTORY_TASK_QUEUE,
        workflows=[_PagedHistoryWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )
    workflow_id = "live-continuation-history"
    async with worker:
        handle = await client.start_workflow(
            _PagedHistoryWorkflow.run,
            25,
            id=workflow_id,
            task_queue=_HISTORY_TASK_QUEUE,
        )
        await handle.result()
    return workflow_id, handle.result_run_id


async def _collect_composite_pages(
    config: Path, tool: str, arguments: dict[str, Any]
) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    token: str | None = None
    for _ in range(20):
        call_arguments = dict(arguments)
        if token is not None:
            call_arguments["continuation_token"] = token
        page = await _call_fresh(config, tool, call_arguments)
        pages.append(page)
        next_token = page["continuation_token"]
        if next_token is None:
            return pages
        assert next_token != token
        token = next_token
    pytest.fail(f"{tool} did not terminate within 20 pages")


async def _collect_visibility_pages(
    config: Path, arguments: dict[str, Any]
) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    token: str | None = None
    for _ in range(20):
        call_arguments = dict(arguments)
        if token is not None:
            call_arguments["continuation_token"] = token
        page = await _call_fresh(config, "search_workflow_runs", call_arguments)
        pages.append(page)
        next_token = page["answer"]["continuation_token"]
        if next_token is None:
            return pages
        assert "." not in next_token
        assert next_token != token
        token = next_token
    pytest.fail("search_workflow_runs did not terminate within 20 pages")


@pytest.mark.asyncio
async def test_all_continuations_survive_mcp_reconstruction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    port = reserve_local_port()
    config = tmp_path / "targets.yaml"
    write_target_config(config, port)
    environment = child_environment(os.environ, port, config)
    orchestrator = FixtureOrchestrator(environment=environment, log_directory=tmp_path)
    monkeypatch.setenv("OPS_MCP_GROUP", "demo-support")
    monkeypatch.setattr(PopulationAdvancedOptions, "resolved", _small_population_limits)

    try:
        server_process = orchestrator.start(
            "temporal-server", build_server_command(_TEMPORAL or "temporal", port)
        )
        wait_for_port(port, server_process)
        orchestrator.apply(fixture_steps("isolated", 1))

        search_args = {
            "target_ref": "acceptance-local",
            "query": "WorkflowType = 'OrderWorkflow'",
            "fields": ["WorkflowId", "RunId", "ExecutionStatus"],
            "page_size": 2,
        }
        search_first = await _call_fresh(config, "search_workflow_runs", search_args)
        search_token = search_first["answer"]["continuation_token"]
        assert search_token and "." not in search_token
        search_second = await _call_fresh(
            config,
            "search_workflow_runs",
            {**search_args, "continuation_token": search_token},
        )
        first_ids = {row["workflow_id"] for row in search_first["answer"]["workflows"]}
        second_ids = {row["workflow_id"] for row in search_second["answer"]["workflows"]}
        assert first_ids and second_ids and first_ids.isdisjoint(second_ids)

        query = "WorkflowType = 'OrderWorkflow'"
        now = datetime.now(UTC)
        composite_calls = (
            ("inspect_workflow_task_failures", {"target_ref": "acceptance-local", "query": query}),
            (
                "inspect_activity_queue_dependencies",
                {
                    "target_ref": "acceptance-local",
                    "query": query,
                    "activity_task_queues": ["orders-demo"],
                },
            ),
            (
                "analyze_incident_impact",
                {
                    "target_ref": "acceptance-local",
                    "candidate_query": query,
                    "started_at": (now - timedelta(hours=1)).isoformat(),
                    "ended_at": (now + timedelta(hours=1)).isoformat(),
                    "affected_activity_task_queues": ["orders-demo"],
                },
            ),
        )
        for tool, arguments in composite_calls:
            pages = await _collect_composite_pages(config, tool, arguments)
            assert len(pages) > 1
            assert pages[0]["continuation_token"]
            assert "." in pages[0]["continuation_token"]
            assert (
                pages[1]["inspection"]["candidate_frame_size"]
                > pages[0]["inspection"]["candidate_frame_size"]
            )
            assert pages[1]["cumulative"]["inspected"] >= pages[0]["cumulative"]["inspected"]
            assert pages[1]["inspection"]["sample_seed"] == pages[0]["inspection"]["sample_seed"]
            assert pages[-1]["continuation_token"] is None
            assert pages[-1]["inspection"]["candidate_collection_complete"] is True
            assert pages[-1]["cumulative"]["inspected"] == sum(
                page["page"]["inspected"] for page in pages
            )
            if tool != "analyze_incident_impact":
                assert (
                    pages[1]["answer"]["effective_query"] == pages[0]["answer"]["effective_query"]
                )
            else:
                assert (
                    pages[1]["impact_criteria"]["incident_window"]
                    == pages[0]["impact_criteria"]["incident_window"]
                )

        history_workflow_id, history_run_id = await _seed_paged_history(port)
        history_args = {
            "target_ref": "acceptance-local",
            "workflow_id": history_workflow_id,
            "run_id": history_run_id,
            "event_id_range": {"first": 1, "last": 1_000_000},
            # Temporal returns a native page larger than this requested maximum. Multiple MCP
            # continuations must resume within that same native page.
            "limit": 20,
        }
        history_pages: list[dict[str, Any]] = []
        history_token: str | None = None
        history_envelopes = 0
        history_native_tokens = 0
        for _ in range(10):
            call_arguments = dict(history_args)
            if history_token is not None:
                call_arguments["continuation_token"] = history_token
            page = await _call_fresh(config, "get_workflow_history_slice", call_arguments)
            history_pages.append(page)
            history_token = page["answer"]["continuation_token"]
            if history_token is None:
                break
            if "." in history_token:
                history_envelopes += 1
            else:
                history_native_tokens += 1
        else:
            pytest.fail("history continuation did not terminate within 10 pages")

        assert len(history_pages) >= 4
        assert history_envelopes >= 3
        assert history_native_tokens >= 1
        event_ids = [
            event["event_id"] for page in history_pages for event in page["answer"]["events"]
        ]
        assert event_ids == list(range(1, event_ids[-1] + 1))
        assert len(event_ids) == len(set(event_ids))
    finally:
        orchestrator.close()


@pytest.mark.asyncio
async def test_large_fixture_continuations_survive_mcp_reconstruction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Page all scaled acceptance populations through newly built MCP servers."""
    port = reserve_local_port()
    config = tmp_path / "targets.yaml"
    write_target_config(config, port)
    environment = child_environment(os.environ, port, config)
    orchestrator = FixtureOrchestrator(environment=environment, log_directory=tmp_path)
    monkeypatch.setenv("OPS_MCP_GROUP", "demo-support")
    monkeypatch.setattr(PopulationAdvancedOptions, "resolved", _large_population_limits)

    try:
        server_process = orchestrator.start(
            "temporal-server", build_server_command(_TEMPORAL or "temporal", port)
        )
        wait_for_port(port, server_process)
        orchestrator.apply(fixture_steps("noisy", 4))
        for worker_name in (
            "session-1-worker",
            "session-2-worker",
            "session-3-v2-worker",
            "session-4-workflow-worker",
            "session-4-recovery-workers",
        ):
            orchestrator.stop(worker_name)
        await asyncio.sleep(1)

        visibility_pages = await _collect_visibility_pages(
            config,
            {
                "target_ref": "acceptance-local",
                "query": "",
                "fields": ["WorkflowId", "RunId", "WorkflowType", "ExecutionStatus"],
                "page_size": 200,
            },
        )
        workflows = [row for page in visibility_pages for row in page["answer"]["workflows"]]
        run_ids = [row["run_id"] for row in workflows]
        workflow_ids = [row["workflow_id"] for row in workflows]
        assert len(workflows) == 2_233
        assert len(run_ids) == len(set(run_ids))
        assert len(workflow_ids) == len(set(workflow_ids))
        assert all(
            workflow_id.startswith(("order-", "session2-", "session3-", "session4-"))
            for workflow_id in workflow_ids
        )
        assert len(visibility_pages) >= 12

        now = datetime.now(UTC)
        large_composites = (
            (
                "inspect_workflow_task_failures",
                {
                    "target_ref": "acceptance-local",
                    "query": "WorkflowType = 'FulfillmentWorkflow'",
                },
                230,
            ),
            (
                "inspect_activity_queue_dependencies",
                {
                    "target_ref": "acceptance-local",
                    "query": "WorkflowType = 'PaymentReconciliationWorkflow'",
                    "activity_task_queues": ["ledger"],
                },
                800,
            ),
            (
                "analyze_incident_impact",
                {
                    "target_ref": "acceptance-local",
                    "candidate_query": "WorkflowId STARTS_WITH 'session4-'",
                    "started_at": (now - timedelta(hours=1)).isoformat(),
                    "ended_at": (now + timedelta(hours=1)).isoformat(),
                    "affected_activity_task_queues": [
                        "session4-ledger-payments",
                        "session4-ledger-core",
                    ],
                },
                160,
            ),
        )
        for tool, arguments, expected_candidates in large_composites:
            pages = await _collect_composite_pages(config, tool, arguments)
            assert len(pages) > 1
            assert pages[-1]["continuation_token"] is None
            assert pages[-1]["inspection"]["candidate_collection_complete"] is True
            assert pages[-1]["inspection"]["candidate_frame_size"] == expected_candidates
            assert pages[-1]["inspection"]["candidate_count"] == expected_candidates
            assert pages[-1]["cumulative"]["inspected"] == sum(
                page["page"]["inspected"] for page in pages
            )
            assert all(
                page["inspection"]["sample_seed"] == pages[0]["inspection"]["sample_seed"]
                for page in pages
            )
    finally:
        orchestrator.close()
