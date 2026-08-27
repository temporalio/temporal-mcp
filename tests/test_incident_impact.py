from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ops_mcp.analysis import classify_incident_impact
from ops_mcp.contracts import (
    AnalyzeIncidentImpactRequest,
    IncidentImpactClassification,
    IncidentTimeWindow,
    PopulationLimits,
)
from ops_mcp.facts import (
    ActivityTaskEventAttributes,
    CountPrecision,
    HistoryEventFact,
    HistoryReadFact,
    IndexedValueTypeFact,
    SearchAttributeFieldFact,
    SearchAttributeSource,
    VisibilityCountFact,
    VisibilityPageFact,
    VisibilitySchemaFact,
    WorkflowExecutionFact,
    WorkflowTaskEventAttributes,
    WorkflowVisibilityFact,
)
from ops_mcp.runtime import BasicBudgetLimits, BasicExecutionBudget, Principal, RequestContext
from ops_mcp.services import DefaultPopulationDiagnosticService
from ops_mcp.targets import ConnectionDescriptor, ResolvedTarget

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
INCIDENT_START = NOW - timedelta(hours=2)
INCIDENT_END = INCIDENT_START + timedelta(minutes=40)


class _Catalog:
    def __init__(self) -> None:
        self.target = ResolvedTarget(
            target_ref="ledger-prod",
            aliases=("ledger prod",),
            account="demo",
            cluster="local",
            namespace="default",
            connection=ConnectionDescriptor(address="localhost:7233", namespace="default"),
        )

    def authorize(self, principal, target_ref):
        del principal, target_ref
        return self.target


class _Visibility:
    def __init__(self, rows: tuple[WorkflowVisibilityFact, ...]) -> None:
        self.rows = rows

    async def count_workflows(self, context, target, *, query, group_by=None):
        del target, group_by
        context.budget.reserve_temporal_call("count")
        return VisibilityCountFact(
            namespace="default",
            query=query,
            group_by=None,
            count=len(self.rows),
            groups=(),
            precision=CountPrecision.APPROXIMATE,
        )

    async def list_workflows_page(self, context, target, *, query, page_size, page_token=None):
        del target
        context.budget.reserve_temporal_call("list")
        context.budget.consume_page("list")
        offset = int(page_token.decode()) if page_token else 0
        rows = self.rows[offset : offset + page_size]
        next_offset = offset + len(rows)
        return VisibilityPageFact(
            namespace="default",
            query=query,
            rows=rows,
            next_page_token=(str(next_offset).encode() if next_offset < len(self.rows) else None),
        )

    async def get_visibility_schema(self, context, target):
        del target
        context.budget.reserve_temporal_call("schema")
        return VisibilitySchemaFact(
            namespace="default",
            fields=(
                SearchAttributeFieldFact(
                    name="Team",
                    source=SearchAttributeSource.CUSTOM,
                    value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
                ),
            ),
            storage_schema=(),
        )


class _Histories:
    async def read(
        self,
        context,
        target,
        workflow_id,
        run_id,
        *,
        page_size=250,
        max_events=None,
        next_page_token=None,
        reverse=False,
    ):
        del target, page_size, max_events, next_page_token
        context.budget.reserve_temporal_call("history")
        context.budget.consume_page("history")
        index = int(workflow_id.removeprefix("ledger-"))
        events: tuple[HistoryEventFact, ...] = ()
        if index % 4 == 0:
            events = (
                HistoryEventFact(
                    event_id=10,
                    event_type="EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
                    event_time=INCIDENT_START - timedelta(minutes=1),
                    task_id=10,
                    attributes=ActivityTaskEventAttributes(
                        phase="scheduled",
                        activity_id="post-ledger",
                        activity_type="PostLedger",
                        task_queue="ledger-activity",
                    ),
                ),
                HistoryEventFact(
                    event_id=11,
                    event_type="EVENT_TYPE_ACTIVITY_TASK_STARTED",
                    event_time=INCIDENT_END + timedelta(minutes=1),
                    task_id=11,
                    attributes=ActivityTaskEventAttributes(phase="started", scheduled_event_id=10),
                ),
            )
        context.budget.consume_history_events(len(events))
        return HistoryReadFact(
            workflow_id=workflow_id,
            run_id=run_id,
            events=events,
            reverse=reverse,
            pages_read=1,
            events_scanned=len(events),
            next_page_token=None,
            observed_at=NOW,
        )

    async def closing_event(self, *args, **kwargs):
        raise AssertionError("not used")


class _Unused:
    def __getattr__(self, name):
        raise AssertionError(f"unexpected gateway use: {name}")


class _Workflows:
    async def describe(self, context, target, workflow_id, run_id):
        del target
        context.budget.reserve_temporal_call("describe")
        index = int(workflow_id.removeprefix("ledger-"))
        row = _row(index)
        return WorkflowExecutionFact(
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_type=row.workflow_type,
            status=row.status or "UNKNOWN",
            task_queue=row.task_queue or "ledger-workflow",
            namespace="default",
            start_time=row.start_time,
            execution_time=row.execution_time,
            close_time=row.close_time,
            history_length=row.history_length or 0,
            history_size_bytes=0,
            first_run_id=run_id,
            parent_workflow_id=None,
            parent_run_id=None,
            root_workflow_id=row.root_workflow_id,
            root_run_id=row.root_run_id,
            observed_at=NOW,
        )


def _row(index: int) -> WorkflowVisibilityFact:
    return WorkflowVisibilityFact(
        namespace="default",
        workflow_id=f"ledger-{index}",
        run_id=f"run-{index}",
        workflow_type="SettlementWorkflow" if index % 2 == 0 else "AuditWorkflow",
        status="COMPLETED",
        task_queue="ledger-workflow",
        start_time=INCIDENT_START - timedelta(minutes=10),
        execution_time=INCIDENT_START - timedelta(minutes=10),
        close_time=INCIDENT_END + timedelta(minutes=10),
        history_length=20,
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id=f"ledger-{index}",
        root_run_id=f"run-{index}",
        search_attributes={"Team": "payments" if index % 2 == 0 else "platform"},
    )


def _context() -> RequestContext:
    return RequestContext(
        request_id="incident-impact-test",
        principal=Principal(subject="demo", groups=("demo",)),
        budget=BasicExecutionBudget(
            deadline=datetime.now(UTC) + timedelta(minutes=5),
            limits=BasicBudgetLimits(
                temporal_calls=250,
                pages=250,
                history_events=10_000,
                output_bytes=2_000_000,
            ),
        ),
    )


async def test_160_visibility_candidates_are_not_promoted_to_confirmed_impact() -> None:
    rows = tuple(_row(index) for index in range(160))
    service = DefaultPopulationDiagnosticService(
        _Catalog(), _Visibility(rows), _Workflows(), _Histories(), _Unused()
    )

    result = await service.analyze_incident_impact(
        _context(),
        AnalyzeIncidentImpactRequest(
            target_ref="ledger-prod",
            query="WorkflowType IS NOT NULL",
            incident_window=IncidentTimeWindow(started_at=INCIDENT_START, ended_at=INCIDENT_END),
            affected_activity_task_queues=("ledger-activity",),
            expected_duration_seconds=24 * 60 * 60,
            limits=PopulationLimits(
                max_candidates=160,
                max_visibility_pages=5,
                max_runs=100,
                max_histories=100,
                max_history_events_per_run=100,
                max_task_queues=1,
                max_concurrency=10,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="incident-trap",
            group_by_custom_search_attributes=("Team",),
        ),
    )

    summary = result.answer.summary
    confirmed_runs = tuple(
        run
        for run in result.answer.runs
        if run.classification is IncidentImpactClassification.CONFIRMED
    )
    assert summary.candidate_frame_size == 160
    assert summary.inspected == 100
    assert summary.uninspected == 60
    assert summary.confirmed_historical_impact == len(confirmed_runs)
    assert 0 < summary.confirmed_historical_impact < summary.inspected
    assert summary.confirmed_affected_lower_bound == len(confirmed_runs)
    assert summary.exact_confirmed_count is False
    assert summary.result_scope == "bounded_inspection"
    assert sum(group.confirmed_runs for group in result.answer.confirmed_groups) == len(
        confirmed_runs
    )
    assert sum(group.confirmed_runs for group in result.answer.confirmed_groups) != 160
    assert result.inspection.completion == "sampled"
    assert any("lower bound" in warning for warning in result.warnings)
    assert any("poller" in warning for warning in result.warnings)


def test_only_affected_queue_failure_promotes_timing_evidence() -> None:
    row = _row(1)
    generic_failure = HistoryEventFact(
        event_id=3,
        event_type="EVENT_TYPE_WORKFLOW_TASK_FAILED",
        event_time=INCIDENT_START + timedelta(minutes=5),
        task_id=3,
        attributes=WorkflowTaskEventAttributes(phase="failed"),
    )
    scheduled = HistoryEventFact(
        event_id=4,
        event_type="EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
        event_time=INCIDENT_START + timedelta(minutes=6),
        task_id=4,
        attributes=ActivityTaskEventAttributes(phase="scheduled", task_queue="ledger-activity"),
    )
    timed_out = HistoryEventFact(
        event_id=5,
        event_type="EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT",
        event_time=INCIDENT_START + timedelta(minutes=7),
        task_id=5,
        attributes=ActivityTaskEventAttributes(phase="timed_out", scheduled_event_id=4),
    )

    generic = classify_incident_impact(
        row,
        HistoryReadFact(
            workflow_id=row.workflow_id,
            run_id=row.run_id,
            events=(generic_failure,),
            reverse=False,
            pages_read=1,
            events_scanned=1,
            next_page_token=None,
            observed_at=NOW,
        ),
        incident_window=IncidentTimeWindow(started_at=INCIDENT_START, ended_at=INCIDENT_END),
        affected_activity_task_queues=frozenset({"ledger-activity"}),
        expected_duration_seconds=24 * 60 * 60,
        observed_at=NOW,
    )
    queue_linked = classify_incident_impact(
        row,
        HistoryReadFact(
            workflow_id=row.workflow_id,
            run_id=row.run_id,
            events=(scheduled, timed_out),
            reverse=False,
            pages_read=1,
            events_scanned=2,
            next_page_token=None,
            observed_at=NOW,
        ),
        incident_window=IncidentTimeWindow(started_at=INCIDENT_START, ended_at=INCIDENT_END),
        affected_activity_task_queues=frozenset({"ledger-activity"}),
        expected_duration_seconds=24 * 60 * 60,
        observed_at=NOW,
    )

    assert generic.classification is IncidentImpactClassification.UNCONFIRMED
    assert queue_linked.classification is IncidentImpactClassification.CONFIRMED
    assert any(
        finding.code == "affected_activity_timed_out_during_incident"
        for finding in queue_linked.findings
    )
