from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from ops_mcp.contracts import (
    AssessWorkflowRecoveryRequest,
    FindStuckWorkflowsRequest,
    FindWorkflowTaskFailuresRequest,
    PopulationLimits,
    PopulationTimeWindow,
)
from ops_mcp.facts import (
    ActivityTaskEventAttributes,
    CountPrecision,
    FailureFact,
    HistoryEventFact,
    HistoryReadFact,
    IndexedValueTypeFact,
    PendingActivityFact,
    PollerFact,
    SearchAttributeFieldFact,
    SearchAttributeSource,
    TaskQueueFact,
    TaskQueueKind,
    VisibilityCountFact,
    VisibilityPageFact,
    VisibilitySchemaFact,
    WorkflowExecutionFact,
    WorkflowTaskEventAttributes,
    WorkflowVisibilityFact,
)
from ops_mcp.mcp.adapters import _CONTINUATIONS, _with_population_continuation
from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    Principal,
    RequestContext,
)
from ops_mcp.services import DefaultPopulationDiagnosticService
from ops_mcp.targets import ConnectionDescriptor, ResolvedTarget

NOW = datetime.now(UTC)


class Catalog:
    def __init__(self, target: ResolvedTarget) -> None:
        self.target = target
        self.authorized: list[str] = []

    def resolve(self, principal: Principal, human_name: str) -> ResolvedTarget:
        del principal, human_name
        return self.target

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        del principal
        self.authorized.append(target_ref)
        return self.target


class Visibility:
    def __init__(
        self,
        rows: tuple[WorkflowVisibilityFact, ...],
        *,
        has_more: bool,
        population_count: int = 100_000,
        schema: VisibilitySchemaFact | None = None,
    ) -> None:
        self.rows = rows
        self.has_more = has_more
        self.population_count = population_count
        self.schema = schema
        self.page_sizes: list[int] = []
        self.queries: list[str] = []
        self.schema_calls = 0

    async def count_workflows(self, context, target, *, query, group_by=None):
        del target, group_by
        context.budget.reserve_temporal_call("count")
        self.queries.append(query)
        return VisibilityCountFact(
            namespace="default",
            query=query,
            group_by=None,
            count=self.population_count,
            groups=(),
            precision=CountPrecision.APPROXIMATE,
        )

    async def list_workflows_page(self, context, target, *, query, page_size, page_token=None):
        del target
        context.budget.reserve_temporal_call("list")
        context.budget.consume_page("list")
        self.page_sizes.append(page_size)
        self.queries.append(query)
        offset = int(page_token.decode("ascii")) if page_token else 0
        rows = self.rows[offset : offset + page_size]
        next_offset = offset + len(rows)
        has_more = next_offset < len(self.rows) or self.has_more
        return VisibilityPageFact(
            namespace="default",
            query=query,
            rows=rows,
            next_page_token=str(next_offset).encode("ascii") if has_more else None,
        )

    async def get_visibility_schema(self, context, target):
        del target
        context.budget.reserve_temporal_call("schema")
        self.schema_calls += 1
        assert self.schema is not None
        return self.schema


class Workflows:
    def __init__(self, values: dict[str, WorkflowExecutionFact]) -> None:
        self.values = values
        self.calls: list[str] = []

    async def describe(self, context, target, workflow_id, run_id=None):
        del target
        context.budget.reserve_temporal_call("describe")
        self.calls.append(f"{workflow_id}:{run_id}")
        return self.values[workflow_id]


class Histories:
    def __init__(self, values: dict[str, HistoryReadFact]) -> None:
        self.values = values
        self.calls: list[tuple[str, int | None]] = []
        self.directions: list[bool] = []

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
        del target, run_id, page_size, next_page_token
        context.budget.reserve_temporal_call("history")
        context.budget.consume_page("history")
        value = self.values[workflow_id]
        context.budget.consume_history_events(len(value.events))
        self.calls.append((workflow_id, max_events))
        self.directions.append(reverse)
        return value

    async def closing_event(self, *args, **kwargs):
        raise AssertionError("not used")


class Queues:
    def __init__(self, values: dict[tuple[str, TaskQueueKind], TaskQueueFact]) -> None:
        self.values = values
        self.calls: list[tuple[str, TaskQueueKind]] = []

    async def describe(self, context, target, task_queue, task_type):
        del target
        context.budget.reserve_temporal_call("queue")
        self.calls.append((task_queue, task_type))
        return self.values[(task_queue, task_type)]


def _target() -> ResolvedTarget:
    return ResolvedTarget(
        target_ref="orders-prod",
        aliases=("orders prod",),
        account="demo",
        cluster="local",
        namespace="default",
        connection=ConnectionDescriptor(address="localhost:7233", namespace="default"),
    )


def _context() -> RequestContext:
    return RequestContext(
        request_id="population-test",
        principal=Principal(subject="demo", groups=("demo",)),
        budget=BasicExecutionBudget(
            deadline=NOW + timedelta(minutes=5),
            limits=BasicBudgetLimits(
                temporal_calls=300, pages=300, history_events=10_000, output_bytes=1_000_000
            ),
        ),
    )


def _window() -> PopulationTimeWindow:
    return PopulationTimeWindow(
        started_at_or_after=NOW - timedelta(days=7),
        started_before=NOW + timedelta(minutes=1),
    )


def _row(index: int, *, task_queue: str = "workflows") -> WorkflowVisibilityFact:
    return WorkflowVisibilityFact(
        namespace="default",
        workflow_id=f"wf-{index}",
        run_id=f"run-{index}",
        workflow_type="ExampleWorkflow",
        status="RUNNING",
        task_queue=task_queue,
        start_time=NOW - timedelta(hours=2),
        execution_time=NOW - timedelta(hours=2),
        close_time=None,
        history_length=20,
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id=f"wf-{index}",
        root_run_id=f"run-{index}",
    )


def _custom_schema(*names: str) -> VisibilitySchemaFact:
    return VisibilitySchemaFact(
        namespace="default",
        fields=tuple(
            SearchAttributeFieldFact(
                name=name,
                source=SearchAttributeSource.CUSTOM,
                value_type=IndexedValueTypeFact(code=2, name="KEYWORD"),
            )
            for name in names
        ),
        storage_schema=(),
    )


def _workflow(index: int, *, pending_activity: bool = False) -> WorkflowExecutionFact:
    return WorkflowExecutionFact(
        workflow_id=f"wf-{index}",
        run_id=f"run-{index}",
        workflow_type="ExampleWorkflow",
        status="RUNNING",
        task_queue="workflows",
        namespace="default",
        start_time=NOW - timedelta(hours=2),
        execution_time=NOW - timedelta(hours=2),
        close_time=None,
        history_length=20,
        history_size_bytes=100,
        first_run_id=f"run-{index}",
        parent_workflow_id=None,
        parent_run_id=None,
        root_workflow_id=f"wf-{index}",
        root_run_id=f"run-{index}",
        pending_activities=(
            PendingActivityFact(
                activity_id="activity-1",
                activity_type="DoWork",
                state="SCHEDULED",
                attempt=1,
                maximum_attempts=3,
                scheduled_time=NOW,
                last_started_time=None,
                next_attempt_time=None,
                last_worker_identity=None,
                last_failure=None,
                paused=False,
                activity_task_queue="activities",
            ),
        )
        if pending_activity
        else (),
    )


def _history(index: int, *, failure: bool = False, activity_queue: bool = False):
    events = []
    if failure:
        events.append(
            HistoryEventFact(
                event_id=7,
                event_type="EVENT_TYPE_WORKFLOW_TASK_FAILED",
                event_time=NOW,
                task_id=1,
                attributes=WorkflowTaskEventAttributes(
                    phase="failed",
                    failure=FailureFact(
                        message="NondeterminismError: command mismatch",
                        stack_trace=(
                            "event 998: expected ScheduleActivityTask; generated StartTimer"
                        ),
                    ),
                ),
            )
        )
    if activity_queue:
        events.append(
            HistoryEventFact(
                event_id=8,
                event_type="EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
                event_time=NOW,
                task_id=2,
                attributes=ActivityTaskEventAttributes(
                    phase="scheduled",
                    activity_id="activity-1",
                    activity_type="DoWork",
                    task_queue="activities",
                ),
            )
        )
    return HistoryReadFact(
        workflow_id=f"wf-{index}",
        run_id=f"run-{index}",
        events=tuple(events),
        reverse=False,
        pages_read=1,
        events_scanned=len(events),
        next_page_token=None,
        observed_at=NOW,
    )


def _queue(name: str, kind: TaskQueueKind, *, pollers: bool) -> TaskQueueFact:
    return TaskQueueFact(
        task_queue=name,
        task_type=kind,
        pollers=(PollerFact(identity="worker", last_access_time=NOW, rate_per_second=1),)
        if pollers
        else (),
        approximate_backlog_count=1,
        approximate_backlog_age_seconds=5,
        tasks_add_rate=1,
        tasks_dispatch_rate=0,
        rate_limiting_active=False,
        observed_at=NOW,
    )


async def test_recovery_composite_uses_exact_activity_outcomes_and_excludes_failed_run() -> None:
    rows = tuple(_row(index) for index in range(3))

    def activity_history(index: int, phase: str) -> HistoryReadFact:
        scheduled = HistoryEventFact(
            event_id=8,
            event_type="EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            event_time=NOW,
            task_id=1,
            attributes=ActivityTaskEventAttributes(
                phase="scheduled",
                activity_id="activity-1",
                activity_type="DoWork",
                task_queue="activities",
            ),
        )
        terminal = HistoryEventFact(
            event_id=9,
            event_type=f"EVENT_TYPE_ACTIVITY_TASK_{phase.upper()}",
            event_time=NOW,
            task_id=2,
            attributes=ActivityTaskEventAttributes(
                phase=phase,
                scheduled_event_id=8,
            ),
        )
        return HistoryReadFact(
            workflow_id=f"wf-{index}",
            run_id=f"run-{index}",
            events=(scheduled, terminal),
            reverse=False,
            pages_read=1,
            events_scanned=2,
            next_page_token=None,
            observed_at=NOW,
        )

    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False, population_count=3),
        Workflows({}),
        Histories(
            {
                "wf-0": activity_history(0, "failed"),
                "wf-1": activity_history(1, "completed"),
                "wf-2": activity_history(2, "completed"),
            }
        ),
        Queues({}),
    )

    result = await service.assess_workflow_recovery(
        _context(),
        AssessWorkflowRecoveryRequest(
            target_ref="orders-prod",
            failed_workflow_id="wf-0",
            failed_run_id="run-0",
            comparison_query="WorkflowType = 'ExampleWorkflow'",
            activity_type="DoWork",
        ),
    )

    assert result.answer.failed_run_activity_outcome == "failure"
    assert result.answer.candidate_runs == 2
    assert result.answer.activity_successes == 2
    assert result.answer.activity_failures == 0
    assert result.answer.recovery_assessment == "supported"
    assert result.inspection.completion == "complete"


async def test_failure_population_narrows_before_bounded_history_fanout() -> None:
    rows = tuple(_row(index) for index in range(4))
    visibility = Visibility(rows, has_more=True)
    workflows = Workflows({f"wf-{i}": _workflow(i) for i in range(4)})
    histories = Histories({f"wf-{i}": _history(i, failure=i == 0) for i in range(4)})
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()), visibility, workflows, histories, Queues({})
    )

    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="WorkflowType = 'ExampleWorkflow'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=4,
                max_runs=4,
                max_histories=4,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
        ),
    )

    assert visibility.page_sizes == [4]
    assert len(workflows.calls) == 4
    assert len(histories.calls) == 4
    assert all(max_events == 20 for _, max_events in histories.calls)
    assert result.inspection.population_count == 100_000
    assert result.inspection.population_count_precision == "approximate"
    assert result.inspection.candidate_frame_size == 4
    assert result.inspection.candidate_frame_has_more is True
    assert result.inspection.sample_size == 4
    assert result.inspection.runs_inspected == 4
    assert result.inspection.histories_inspected == 4
    assert result.inspection.completion == "sampled"
    assert result.answer.failure_groups[0].failure_class == "nondeterminism"
    assert result.answer.failure_groups[0].runs == 1
    assert "StartTime" in result.answer.effective_query


async def test_failure_aggregation_is_typed_complete_and_confirmed_only() -> None:
    rows = tuple(
        replace(
            _row(index),
            workflow_type="FulfillmentWorkflow",
            search_attributes={"Team": "shipping" if index < 2 else "platform"},
        )
        for index in range(3)
    )
    visibility = Visibility(rows, has_more=False, schema=_custom_schema("Team"))
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        visibility,
        Workflows({f"wf-{index}": _workflow(index) for index in range(3)}),
        Histories({f"wf-{index}": _history(index, failure=index in {0, 2}) for index in range(3)}),
        Queues({}),
    )

    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="WorkflowType = 'FulfillmentWorkflow'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=3,
                max_runs=3,
                max_histories=3,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
            group_by_custom_search_attributes=("Team",),
        ),
    )

    aggregation = result.answer.affected_aggregation
    assert visibility.schema_calls == 1
    assert aggregation.scope == "complete_query_population"
    assert aggregation.dimensions == ("workflow_type", "Team")
    assert aggregation.confirmed_affected_runs == 2
    assert sum(group.confirmed_runs for group in aggregation.groups) == 2
    assert {group.custom_search_attributes[0].value for group in aggregation.groups} == {
        "shipping",
        "platform",
    }


async def test_population_resume_starts_at_signed_candidate_frame_cursor() -> None:
    rows = tuple(_row(index) for index in range(6))
    visibility = Visibility(rows, has_more=False)
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        visibility,
        Workflows({f"wf-{index}": _workflow(index) for index in range(6)}),
        Histories({f"wf-{index}": _history(index) for index in range(6)}),
        Queues({}),
    )

    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="WorkflowType = 'FulfillmentWorkflow'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=2,
                max_runs=2,
                max_histories=2,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
            candidate_page_token="Mg",
        ),
    )

    assert {run.workflow_id for run in result.answer.runs} == {"wf-2", "wf-3"}
    assert result.inspection.candidate_next_page_token == "NA"

    preferred = _with_population_continuation(
        result,
        state=None,
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        scope="scope",
        sample_seed="fixed",
        offset=0,
        started_at=_window().started_at_or_after,
        ended_at=_window().started_before,
        confirmed=lambda run: bool(run.findings),
        candidate_page_token=None,
    )
    state = _CONTINUATIONS.decode(
        preferred.continuation_token,
        tool="inspect_workflow_task_failures",
        target_ref="orders-prod",
        query="scope",
    )
    assert state.candidate_page_token == "NA"


async def test_three_call_population_chain_reports_230_cumulative_runs() -> None:
    rows = tuple(_row(index) for index in range(230))
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False, population_count=230),
        Workflows({f"wf-{index}": _workflow(index) for index in range(230)}),
        Histories({f"wf-{index}": _history(index, failure=True) for index in range(230)}),
        Queues({}),
    )
    limits = PopulationLimits(
        max_candidates=230,
        max_visibility_pages=10,
        max_runs=100,
        max_histories=100,
        max_history_events_per_run=20,
        max_task_queues=1,
        max_concurrency=10,
    )
    state = None
    preferred = None
    for _ in range(3):
        result = await service.find_workflow_task_failures(
            _context(),
            FindWorkflowTaskFailuresRequest(
                target_ref="orders-prod",
                query="WorkflowType = 'ExampleWorkflow'",
                time_window=_window(),
                limits=limits,
                sampling_strategy="deterministic_hash",
                sample_seed=state.sample_seed if state else "fixed",
                excluded_run_identities=state.inspected_run_ledger if state else (),
                candidate_page_token=state.candidate_page_token if state else None,
            ),
        )
        preferred = _with_population_continuation(
            result,
            state=state,
            tool="inspect_workflow_task_failures",
            target_ref="orders-prod",
            scope="230-run-scope",
            sample_seed="fixed",
            offset=state.inspection_offset if state else 0,
            started_at=_window().started_at_or_after,
            ended_at=_window().started_before,
            confirmed=lambda run: bool(run.findings),
            candidate_page_token=state.candidate_page_token if state else None,
        )
        if preferred.continuation_token:
            state = _CONTINUATIONS.decode(
                preferred.continuation_token,
                tool="inspect_workflow_task_failures",
                target_ref="orders-prod",
                query="230-run-scope",
            )

    assert preferred is not None
    assert preferred.continuation_token is None
    assert preferred.next_call is None
    assert preferred.page.inspected == 30
    assert preferred.cumulative.inspected == 230
    assert preferred.cumulative.confirmed == 230
    assert len(preferred.cumulative_confirmed_identities) == 230
    assert preferred.inspection.completion == "complete"


async def test_stuck_population_deduplicates_current_queue_snapshots() -> None:
    rows = (_row(0), _row(1))
    visibility = Visibility(rows, has_more=False)
    workflows = Workflows({"wf-0": _workflow(0, pending_activity=True), "wf-1": _workflow(1)})
    histories = Histories({"wf-0": _history(0, activity_queue=True), "wf-1": _history(1)})
    queue_gateway = Queues(
        {
            ("workflows", TaskQueueKind.WORKFLOW): _queue(
                "workflows", TaskQueueKind.WORKFLOW, pollers=True
            ),
            ("activities", TaskQueueKind.ACTIVITY): _queue(
                "activities", TaskQueueKind.ACTIVITY, pollers=False
            ),
        }
    )
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()), visibility, workflows, histories, queue_gateway
    )

    result = await service.find_stuck_workflows(
        _context(),
        FindStuckWorkflowsRequest(
            target_ref="orders-prod",
            query="ExecutionStatus = 'Running'",
            time_window=_window(),
            minimum_open_duration_seconds=60,
            limits=PopulationLimits(
                max_candidates=2,
                max_runs=2,
                max_histories=2,
                max_history_events_per_run=20,
                max_task_queues=2,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
        ),
    )

    assert queue_gateway.calls == [
        ("activities", TaskQueueKind.ACTIVITY),
        ("workflows", TaskQueueKind.WORKFLOW),
    ]
    assert result.inspection.task_queues_inspected == 2
    assert result.inspection.completion == "complete"
    run = next(item for item in result.answer.runs if item.workflow_id == "wf-0")
    finding = next(item for item in run.findings if item.code == "dispatch_blocked_now")
    assert NOW.isoformat() in finding.caveats[0]
    assert any("diagnostic workflow-query" in warning for warning in result.warnings)


async def test_activity_queue_blast_radius_never_joins_on_workflow_task_queue() -> None:
    rows = tuple(
        replace(
            _row(index, task_queue="ledger"),
            search_attributes={"Team": "ledger" if index < 2 else "unconfirmed-team"},
        )
        for index in range(4)
    )
    workflows = {
        f"wf-{index}": replace(
            _workflow(index, pending_activity=index < 2),
            task_queue="ledger",
            pending_activities=(
                tuple(
                    replace(activity, activity_task_queue="ledger")
                    for activity in _workflow(index, pending_activity=True).pending_activities
                )
                if index < 2
                else ()
            ),
        )
        for index in range(4)
    }
    visibility = Visibility(
        rows,
        has_more=True,
        population_count=1_830,
        schema=_custom_schema("Team"),
    )
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        visibility,
        Workflows(workflows),
        Histories({f"wf-{index}": _history(index) for index in range(4)}),
        Queues(
            {
                ("ledger", TaskQueueKind.WORKFLOW): _queue(
                    "ledger", TaskQueueKind.WORKFLOW, pollers=True
                ),
                ("ledger", TaskQueueKind.ACTIVITY): _queue(
                    "ledger", TaskQueueKind.ACTIVITY, pollers=False
                ),
            }
        ),
    )

    result = await service.find_stuck_workflows(
        _context(),
        FindStuckWorkflowsRequest(
            target_ref="orders-prod",
            query="TaskQueue = 'ledger'",
            time_window=_window(),
            minimum_open_duration_seconds=60,
            activity_task_queue="ledger",
            limits=PopulationLimits(
                max_candidates=4,
                max_runs=4,
                max_histories=4,
                max_history_events_per_run=20,
                max_task_queues=2,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
            group_by_custom_search_attributes=("Team",),
        ),
    )

    assert result.inspection.population_count == 1_830
    assert result.answer.blast_radius is not None
    assert result.answer.blast_radius.confirmed_affected == 2
    assert result.answer.blast_radius.inspected_but_unconfirmed == 2
    assert result.answer.blast_radius.collection_complete is False
    assert result.answer.blast_radius.confirmed_groups[0].confirmed_runs == 2
    assert sum(run.activity_queue_membership == "confirmed" for run in result.answer.runs) == 2
    assert result.answer.affected_aggregation is not None
    assert result.answer.affected_aggregation.scope == "bounded_inspection"
    assert result.answer.affected_aggregation.confirmed_affected_runs == 2
    assert len(result.answer.affected_aggregation.groups) == 1
    assert (
        result.answer.affected_aggregation.groups[0].custom_search_attributes[0].value == "ledger"
    )


async def test_history_limit_is_explicit_and_prevents_complete_claim() -> None:
    rows = tuple(_row(index) for index in range(3))
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False),
        Workflows({f"wf-{i}": _workflow(i) for i in range(3)}),
        Histories({f"wf-{i}": _history(i) for i in range(3)}),
        Queues({}),
    )

    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="WorkflowType = 'ExampleWorkflow'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=3,
                max_runs=3,
                max_histories=1,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=1,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="fixed",
        ),
    )

    assert result.inspection.sample_size == 3
    assert result.inspection.histories_inspected == 1
    assert result.inspection.completion == "truncated"
    assert (
        sum(
            finding.code == "history_not_inspected_due_to_limit"
            for run in result.answer.runs
            for finding in run.findings
        )
        == 2
    )


async def test_complete_multipage_frame_is_sampled_after_all_1900_candidates() -> None:
    rows = tuple(
        replace(
            _row(index),
            workflow_type=("FirstPageWorkflow" if index < 200 else f"Type{index % 3}Workflow"),
        )
        for index in range(1_900)
    )
    histories = Histories({row.workflow_id: _history(index) for index, row in enumerate(rows)})
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False, population_count=1_900),
        Workflows(
            {
                row.workflow_id: replace(_workflow(index), workflow_type=row.workflow_type)
                for index, row in enumerate(rows)
            }
        ),
        histories,
        Queues({}),
    )
    context = _context()
    result = await service.find_workflow_task_failures(
        context,
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="BuildIds = 'build-v2'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=1_900,
                max_visibility_pages=10,
                max_runs=20,
                max_histories=20,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=5,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="frame-wide",
        ),
    )

    assert result.inspection.candidate_frame_size == 1_900
    assert result.inspection.candidate_collection_pages == 10
    assert result.inspection.candidate_collection_complete is True
    assert result.inspection.sampling_frame == "complete_visibility_query"
    assert any(run.workflow_type != "FirstPageWorkflow" for run in result.answer.runs)
    assert histories.directions == [True] * 20
    assert result.inspection.history_direction == "reverse_recent"
    assert result.inspection.estimate_eligible is True
    assert result.inspection.estimate_eligibility_reason == ("complete_frame_uniform_sample")


async def test_truncated_candidate_frame_forbids_extrapolation() -> None:
    rows = tuple(_row(index) for index in range(500))
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False, population_count=500),
        Workflows({row.workflow_id: _workflow(index) for index, row in enumerate(rows)}),
        Histories({row.workflow_id: _history(index) for index, row in enumerate(rows)}),
        Queues({}),
    )
    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="ExecutionStatus = 'Running'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=300,
                max_visibility_pages=2,
                max_runs=5,
                max_histories=1,
                max_history_events_per_run=20,
                max_task_queues=1,
                max_concurrency=2,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="bounded",
        ),
    )

    assert result.inspection.candidate_frame_size == 300
    assert result.inspection.candidate_collection_complete is False
    assert result.inspection.candidate_collection_stop_reason == "max_candidates"
    assert result.inspection.sampling_frame == "bounded_visibility_query"
    assert result.inspection.estimate_eligible is False
    assert result.inspection.estimate_eligibility_reason == "truncated_candidate_frame"
    assert any("must not be extrapolated" in warning for warning in result.warnings)


async def test_recent_reverse_history_preserves_nondeterminism_divergence() -> None:
    history = replace(_history(0, failure=True), next_page_token=b"older-events")
    histories = Histories({"wf-0": history})
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility((_row(0),), has_more=False, population_count=1),
        Workflows({"wf-0": _workflow(0)}),
        histories,
        Queues({}),
    )
    result = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            target_ref="orders-prod",
            query="BuildIds = 'build-v2'",
            time_window=_window(),
            limits=PopulationLimits(
                max_candidates=1,
                max_runs=1,
                max_histories=1,
                max_history_events_per_run=1,
                max_task_queues=1,
                max_concurrency=1,
            ),
            sampling_strategy="deterministic_hash",
            sample_seed="recent-tail",
        ),
    )

    finding = result.answer.runs[0].findings[0]
    assert histories.directions == [True]
    assert finding.code == "workflow_nondeterminism"
    assert finding.evidence == ("history:event:7",)
    assert finding.failure_messages == ("NondeterminismError: command mismatch",)
    assert finding.stack_trace_excerpt == (
        "event 998: expected ScheduleActivityTask; generated StartTimer"
    )
    assert result.inspection.history_direction == "reverse_recent"
    assert result.inspection.history_window_truncated is True


async def test_resumed_failure_inspection_excludes_prior_identity_ledger() -> None:
    rows = tuple(_row(index) for index in range(4))
    service = DefaultPopulationDiagnosticService(
        Catalog(_target()),
        Visibility(rows, has_more=False, population_count=4),
        Workflows({row.workflow_id: _workflow(index) for index, row in enumerate(rows)}),
        Histories({row.workflow_id: _history(index) for index, row in enumerate(rows)}),
        Queues({}),
    )
    base = dict(
        target_ref="orders-prod",
        query="ExecutionStatus = 'Running'",
        time_window=_window(),
        limits=PopulationLimits(
            max_candidates=4,
            max_runs=2,
            max_histories=2,
            max_history_events_per_run=20,
            max_task_queues=1,
            max_concurrency=2,
        ),
        sampling_strategy="deterministic_hash",
        sample_seed="resume",
    )

    first = await service.find_workflow_task_failures(
        _context(), FindWorkflowTaskFailuresRequest(**base)
    )
    first_ids = tuple(f"{run.workflow_id}\0{run.run_id}" for run in first.answer.runs)
    second = await service.find_workflow_task_failures(
        _context(),
        FindWorkflowTaskFailuresRequest(
            **base,
            excluded_run_identities=first_ids,
        ),
    )

    assert first_ids
    assert set(first_ids).isdisjoint(
        f"{run.workflow_id}\0{run.run_id}" for run in second.answer.runs
    )
