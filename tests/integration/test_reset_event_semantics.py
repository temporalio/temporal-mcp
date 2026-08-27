"""Pinned Temporal reset event semantics.

Run against an otherwise disposable local dev server:

    OPS_MCP_INTEGRATION=1 uv run pytest \
      tests/integration/test_reset_event_semantics.py -q

The test deliberately creates reset runs. It does not exercise the MCP reset
planner or grant it write access.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from datetime import timedelta
from importlib.metadata import version
from uuid import uuid4

import pytest
from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    GetSystemInfoRequest,
    PollWorkflowTaskQueueRequest,
)
from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_INTEGRATION") != "1",
    reason="set OPS_MCP_INTEGRATION=1 with Temporal CLI 1.7.0 dev server running",
)


@workflow.defn
class ResetSemanticsWorkflow:
    @workflow.run
    async def run(self) -> str:
        return "complete"


async def _events(client: Client, workflow_id: str, run_id: str):
    return [
        event
        async for event in client.get_workflow_handle(
            workflow_id, run_id=run_id
        ).fetch_history_events()
    ]


async def _wait_for_event(
    client: Client,
    workflow_id: str,
    run_id: str,
    event_type: EventType,
    *,
    timeout_seconds: float = 8,
):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        events = await _events(client, workflow_id, run_id)
        match = next((event for event in events if event.event_type == event_type), None)
        if match:
            return match
        await asyncio.sleep(0.1)
    pytest.fail(f"history never contained {EventType.Name(event_type)}")


def _reset(
    *,
    address: str,
    namespace: str,
    workflow_id: str,
    run_id: str,
    event_id: int,
    reason: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "temporal",
            "--address",
            address,
            "--namespace",
            namespace,
            "workflow",
            "reset",
            "--workflow-id",
            workflow_id,
            "--run-id",
            run_id,
            "--event-id",
            str(event_id),
            "--reason",
            reason,
            "--reapply-exclude",
            "All",
            "--output",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


@pytest.mark.asyncio
async def test_cli_accepts_exact_workflow_task_finish_event_ids() -> None:
    address = os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    cli_version = (
        await asyncio.to_thread(
            subprocess.run,
            ["temporal", "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    ).stdout
    assert "temporal version 1.7.0" in cli_version.lower()
    assert "Server 1.31.0" in cli_version
    assert version("temporalio") == "1.31.0"

    client = await Client.connect(address, namespace=namespace)
    system_info = await client.workflow_service.get_system_info(GetSystemInfoRequest())
    assert system_info.server_version == "1.31.0"

    fixture = uuid4().hex
    workflow_id = f"reset-semantics-completed-{fixture}"
    task_queue = f"reset-semantics-worker-{fixture}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ResetSemanticsWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        handle = await client.start_workflow(
            ResetSemanticsWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )
        assert await handle.result() == "complete"

    base_events = await _events(client, workflow_id, handle.result_run_id)
    completed = next(
        event
        for event in base_events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    )
    closing = next(
        event
        for event in base_events
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    )

    completed_reset = await asyncio.to_thread(
        _reset,
        address=address,
        namespace=namespace,
        workflow_id=workflow_id,
        run_id=handle.result_run_id,
        event_id=completed.event_id,
        reason="prove WorkflowTaskCompleted",
    )
    assert completed_reset.returncode == 0, completed_reset.stderr
    completed_reset_run_id = json.loads(completed_reset.stdout)["runId"]

    reset_failure = await _wait_for_event(
        client,
        workflow_id,
        completed_reset_run_id,
        EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED,
    )
    failed_reset = await asyncio.to_thread(
        _reset,
        address=address,
        namespace=namespace,
        workflow_id=workflow_id,
        run_id=completed_reset_run_id,
        event_id=reset_failure.event_id,
        reason="prove WorkflowTaskFailed",
    )
    assert failed_reset.returncode == 0, failed_reset.stderr

    non_finish_reset = await asyncio.to_thread(
        _reset,
        address=address,
        namespace=namespace,
        workflow_id=workflow_id,
        run_id=handle.result_run_id,
        event_id=closing.event_id,
        reason="reject non-finish event",
    )
    assert non_finish_reset.returncode != 0
    assert "must be at the boundary" in non_finish_reset.stderr

    timeout_workflow_id = f"reset-semantics-timeout-{fixture}"
    timeout_queue = f"reset-semantics-timeout-queue-{fixture}"
    timeout_handle = await client.start_workflow(
        "ResetTimeoutWorkflow",
        id=timeout_workflow_id,
        task_queue=timeout_queue,
        task_timeout=timedelta(seconds=1),
    )
    polled = await client.workflow_service.poll_workflow_task_queue(
        PollWorkflowTaskQueueRequest(
            namespace=namespace,
            task_queue=TaskQueue(name=timeout_queue),
            identity="reset-semantics-test",
        ),
        timeout=timedelta(seconds=5),
    )
    assert polled.task_token
    timed_out = await _wait_for_event(
        client,
        timeout_workflow_id,
        timeout_handle.result_run_id,
        EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
    )
    timed_out_reset = await asyncio.to_thread(
        _reset,
        address=address,
        namespace=namespace,
        workflow_id=timeout_workflow_id,
        run_id=timeout_handle.result_run_id,
        event_id=timed_out.event_id,
        reason="prove WorkflowTaskTimedOut",
    )
    assert timed_out_reset.returncode == 0, timed_out_reset.stderr
