"""Thin FastMCP adapters for approved direct-read services."""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Annotated, Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field, ValidationError

from ops_mcp.contracts import (
    AnalyzeIncidentImpactRequest,
    AnalyzeIncidentImpactResponse,
    CheckConnectionRequest,
    CheckConnectionResponse,
    CountWorkflowsRequest,
    CountWorkflowsResponse,
    DescribeScheduleRequest,
    DescribeScheduleResponse,
    DescribeTaskQueueRequest,
    DescribeTaskQueueResponse,
    DescribeWorkflowRequest,
    DescribeWorkflowResponse,
    DiagnoseWorkflowRequest,
    DiagnoseWorkflowResponse,
    FailureClass,
    FilterWorkflowsRequest,
    FilterWorkflowsResponse,
    FindStuckWorkflowsRequest,
    FindStuckWorkflowsResponse,
    FindWorkflowTaskFailuresRequest,
    FindWorkflowTaskFailuresResponse,
    GetVisibilitySchemaRequest,
    GetVisibilitySchemaResponse,
    GetWorkflowResultRequest,
    GetWorkflowResultResponse,
    HistorySliceRequest,
    HistorySliceResponse,
    IncidentTimeWindow,
    InspectActivityQueueDependenciesResponse,
    InspectWorkflowExecutionRequest,
    InspectWorkflowExecutionResponse,
    InspectWorkflowTaskFailuresResponse,
    ListApprovedWorkflowQueriesRequest,
    ListApprovedWorkflowQueriesResponse,
    ListBatchOperationsRequest,
    ListBatchOperationsResponse,
    ListSchedulesRequest,
    ListSchedulesResponse,
    PlanWorkflowResetRequest,
    PlanWorkflowResetResponse,
    PopulationAdvancedOptions,
    PopulationLimits,
    PopulationTimeWindow,
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    ResolveTargetRequest,
    ResolveTargetResponse,
    SamplingStrategy,
    SearchWorkflowRunsAnswer,
    SearchWorkflowRunsResponse,
    WorkflowCountGroupBy,
    WorkflowField,
)
from ops_mcp.contracts.workflow_reads import EventIdRange
from ops_mcp.mcp.composition import CompositionRoot
from ops_mcp.mcp.errors import (
    raise_invalid_argument,
    raise_request_validation_error,
    raise_service_error,
)
from ops_mcp.runtime import (
    BasicBudgetLimits,
    ContinuationCodec,
    ContinuationState,
    OpsMcpError,
    RequestContext,
    normalize_query,
)

READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)
_CONTINUATIONS = ContinuationCodec()
TargetRefInput = Annotated[
    str, Field(description="Opaque target reference returned by resolve_target.")
]
WorkflowIdInput = Annotated[str, Field(description="Temporal Workflow ID.")]
RunIdInput = Annotated[str, Field(description="Exact Temporal Run ID.")]
VisibilityQueryInput = Annotated[
    str, Field(description="Raw Temporal Visibility query for the intended population.")
]


def register_direct_read_tools(server: FastMCP[CompositionRoot], root: CompositionRoot) -> None:
    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def check_connection(
        target_ref: TargetRefInput,
        probe_codec: Annotated[
            bool, Field(description="Also probe the configured payload codec endpoint.")
        ] = True,
    ) -> CheckConnectionResponse:
        """Use for connectivity questions or connection-shaped failures, not routine preflight."""
        try:
            request = CheckConnectionRequest(
                target_ref=target_ref,
                probe_codec=probe_codec,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.connection.check_connection(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def resolve_target(
        human_name: Annotated[
            str | None,
            Field(description="Exact human target name or alias; omit to list authorized targets."),
        ] = None,
    ) -> ResolveTargetResponse:
        """Resolve once per conversation, then reuse the returned target_ref."""
        try:
            request = ResolveTargetRequest(human_name=human_name)
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return root.services.targets.resolve_target(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def search_workflow_runs(
        target_ref: Annotated[str, Field(description="Target reference from resolve_target.")],
        query: Annotated[
            str,
            Field(
                description="Raw Temporal Visibility query.",
                examples=["ExecutionStatus = 'Running'"],
            ),
        ],
        fields: Annotated[
            list[WorkflowField] | None,
            Field(description="Projected fields; common CamelCase Temporal names are accepted."),
        ] = None,
        custom_search_attributes: Annotated[
            list[str] | None,
            Field(description="Registered custom Search Attributes to project."),
        ] = None,
        page_size: Annotated[int, Field(ge=1, le=200, description="Maximum rows returned.")] = 100,
        continuation_token: Annotated[
            str | None,
            Field(description="Opaque continuation token returned by this search."),
        ] = None,
    ) -> SearchWorkflowRunsResponse:
        """Search one bounded page of workflow runs; use count_workflows for population size."""
        try:
            request = FilterWorkflowsRequest(
                target_ref=target_ref,
                query=query,
                fields=(
                    tuple(fields)
                    if fields
                    else FilterWorkflowsRequest.model_fields["fields"].default
                ),
                custom_search_attributes=tuple(custom_search_attributes or ()),
                page_size=page_size,
                page_token=continuation_token,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            response = await root.services.visibility.filter_workflows(context, request)
            return SearchWorkflowRunsResponse(
                target_ref=response.target_ref,
                answer=SearchWorkflowRunsAnswer(
                    query=response.answer.query,
                    custom_search_attributes=response.answer.custom_search_attributes,
                    workflows=response.answer.workflows,
                    returned=response.answer.returned,
                    continuation_token=response.answer.next_page_token,
                ),
                inspection=response.inspection,
                warnings=response.warnings,
            )
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def filter_workflows(
        target_ref: str,
        query: str,
        fields: list[WorkflowField],
        custom_search_attributes: list[str] | None = None,
        page_size: Annotated[int, Field(ge=1, le=200)] = 100,
        page_token: str | None = None,
    ) -> FilterWorkflowsResponse:
        """Deprecated alias: use search_workflow_runs for Visibility searches."""
        try:
            request = FilterWorkflowsRequest(
                target_ref=target_ref,
                query=query,
                fields=tuple(fields),
                custom_search_attributes=tuple(custom_search_attributes or ()),
                page_size=page_size,
                page_token=page_token,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.visibility.filter_workflows(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def count_workflows(
        target_ref: TargetRefInput,
        query: VisibilityQueryInput,
        group_by: Annotated[
            WorkflowCountGroupBy | None,
            Field(description="Optional server-supported Visibility grouping dimension."),
        ] = None,
    ) -> CountWorkflowsResponse:
        """Count the supplied Visibility population; this does not inspect workflow histories."""
        try:
            request = CountWorkflowsRequest(
                target_ref=target_ref,
                query=query,
                group_by=group_by,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.visibility.count_workflows(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def get_visibility_schema(target_ref: TargetRefInput) -> GetVisibilitySchemaResponse:
        """Use before uncertain system/custom fields; returns types and query examples."""
        try:
            request = GetVisibilitySchemaRequest(target_ref=target_ref)
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.visibility.get_visibility_schema(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def describe_workflow(
        target_ref: TargetRefInput,
        workflow_id: WorkflowIdInput,
        run_id: Annotated[
            str | None, Field(description="Exact Run ID; omit to describe the current run.")
        ] = None,
    ) -> DescribeWorkflowResponse:
        """Describe a run; task_queue is workflow-task dispatch, activities name their queues."""
        try:
            request = DescribeWorkflowRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.describe_workflow.describe_workflow(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def get_workflow_history_slice(
        target_ref: Annotated[str, Field(description="Target reference from resolve_target.")],
        workflow_id: Annotated[str, Field(description="Temporal Workflow ID.")],
        run_id: Annotated[str, Field(description="Exact Temporal Run ID.")],
        failures_only: Annotated[
            Literal[True] | None,
            Field(description="Failure selector; mutually exclusive with range and last_n."),
        ] = None,
        event_id_range: Annotated[
            EventIdRange | None,
            Field(description="Inclusive event range; mutually exclusive with other selectors."),
        ] = None,
        last_n: Annotated[
            int | None,
            Field(ge=1, le=500, description="Recent tail; cannot use a continuation token."),
        ] = None,
        event_types: Annotated[
            list[str] | None,
            Field(description="Optional event-type filter applied within the chosen selector."),
        ] = None,
        continuation_token: Annotated[
            str | None, Field(description="Opaque continuation returned by this tool.")
        ] = None,
        limit: Annotated[
            int, Field(ge=1, le=500, description="Maximum history events returned.")
        ] = 100,
    ) -> HistorySliceResponse:
        """Return a bounded failures, event-range, or last-N slice of exact-run history."""
        try:
            request = HistorySliceRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                failures_only=failures_only,
                event_id_range=event_id_range,
                last_n=last_n,
                event_types=tuple(event_types or ()),
                page_token=continuation_token,
                limit=limit,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.workflow_history.get_workflow_history_slice(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def get_workflow_result(
        target_ref: TargetRefInput,
        workflow_id: WorkflowIdInput,
        run_id: RunIdInput,
        wait: Annotated[
            bool, Field(description="Wait briefly for a running workflow; defaults to false.")
        ] = False,
        wait_timeout_seconds: Annotated[
            float | None,
            Field(gt=0, le=30, description="Bounded wait timeout; requires wait=true."),
        ] = None,
    ) -> GetWorkflowResultResponse:
        """Use for terminal-outcome questions; never waits unless explicitly requested."""
        try:
            request = GetWorkflowResultRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                wait=wait,
                wait_timeout_seconds=wait_timeout_seconds,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.workflow_result.get_workflow_result(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def describe_task_queue(
        target_ref: TargetRefInput,
        task_queue: Annotated[str, Field(description="Exact Temporal task-queue name.")],
        task_type: Annotated[
            Literal["workflow", "activity"],
            Field(description="Explicit queue kind; workflow and activity queues are distinct."),
        ],
    ) -> DescribeTaskQueueResponse:
        """Describe a current queue snapshot, never historical queue health."""
        try:
            request = DescribeTaskQueueRequest(
                target_ref=target_ref,
                task_queue=task_queue,
                task_type=task_type,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.task_queue.describe_task_queue(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def inspect_workflow_execution(
        target_ref: Annotated[str, Field(description="Target reference from resolve_target.")],
        workflow_id: Annotated[str, Field(description="Temporal Workflow ID.")],
        run_id: Annotated[
            str | None,
            Field(description="Exact Run ID; omit to resolve the current run once."),
        ] = None,
        expected_duration_seconds: Annotated[
            float | None,
            Field(gt=0, description="Optional operator-supplied normal duration."),
        ] = None,
        history_event_limit: Annotated[
            int,
            Field(ge=1, le=20_000, description="Bounded history event limit."),
        ] = 2_000,
        include_diagnostic_query: Annotated[
            bool,
            Field(description="Use an approved diagnostic query when configured."),
        ] = True,
    ) -> InspectWorkflowExecutionResponse:
        """Inspect one run's duration, wait, failure, and current dispatch evidence."""
        try:
            request = InspectWorkflowExecutionRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                expected_duration_seconds=expected_duration_seconds,
                history_event_limit=history_event_limit,
                include_diagnostic_query=include_diagnostic_query,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create(limits=BasicBudgetLimits(history_events=history_event_limit))
        try:
            return await root.services.diagnose_workflow.inspect_workflow_execution(
                context, request
            )
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def diagnose_workflow(
        target_ref: str,
        workflow_id: str,
        run_id: str,
        history_event_limit: Annotated[int, Field(ge=1, le=20_000)] = 2_000,
        expected_duration_seconds: Annotated[float | None, Field(gt=0)] = None,
    ) -> DiagnoseWorkflowResponse:
        """Deprecated alias: use inspect_workflow_execution for one-run diagnosis."""
        try:
            request = DiagnoseWorkflowRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                history_event_limit=history_event_limit,
                expected_duration_seconds=expected_duration_seconds,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create(limits=BasicBudgetLimits(history_events=history_event_limit))
        try:
            return await root.services.diagnose_workflow.diagnose_workflow(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def plan_workflow_reset(
        target_ref: TargetRefInput,
        workflow_id: WorkflowIdInput,
        run_id: RunIdInput,
        operator_rationale: Annotated[
            str | None,
            Field(
                min_length=1,
                max_length=500,
                description="Optional operator rationale embedded in the non-executed preview.",
            ),
        ] = None,
    ) -> PlanWorkflowResetResponse:
        """Analyze reset points and replay impact for one exact run; never execute a reset."""
        try:
            request = PlanWorkflowResetRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                operator_rationale=operator_rationale,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.plan_workflow_reset.plan_workflow_reset(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def inspect_workflow_task_failures(
        target_ref: Annotated[str, Field(description="Target reference from resolve_target.")],
        query: Annotated[str, Field(description="Raw Temporal Visibility population query.")],
        started_at: Annotated[
            datetime | None,
            Field(description="Optional inclusive UTC population start time."),
        ] = None,
        ended_at: Annotated[
            datetime | None,
            Field(description="Optional exclusive UTC population end time."),
        ] = None,
        failure_class: Annotated[
            FailureClass | None,
            Field(description="Optional failure class filter applied to inspected history."),
        ] = None,
        group_by_custom_search_attributes: Annotated[
            list[str] | None,
            Field(description="Registered custom Search Attributes used for confirmed grouping."),
        ] = None,
        advanced_options: Annotated[
            PopulationAdvancedOptions | None,
            Field(description="Optional expert overrides on server-owned inspection defaults."),
        ] = None,
        continuation_token: Annotated[
            str | None,
            Field(description="Opaque token returned by a prior call with the same scope."),
        ] = None,
    ) -> InspectWorkflowTaskFailuresResponse:
        """Inspect bounded recent workflow-task failure evidence across a population."""
        now = datetime.now(UTC)
        try:
            if (started_at is None) != (ended_at is None):
                raise ValueError("started_at and ended_at must be supplied together")
            limits = (advanced_options or PopulationAdvancedOptions()).resolved()
            scope = _population_scope(
                query,
                started_at,
                ended_at,
                failure_class,
                tuple(group_by_custom_search_attributes or ()),
                limits,
            )
            state = (
                _CONTINUATIONS.decode(
                    continuation_token,
                    tool="inspect_workflow_task_failures",
                    target_ref=target_ref,
                    query=scope,
                )
                if continuation_token
                else None
            )
            effective_started_at = (
                datetime.fromisoformat(state.started_at)
                if state and state.started_at
                else started_at
            ) or datetime(1970, 1, 1, tzinfo=UTC)
            effective_ended_at = (
                datetime.fromisoformat(state.ended_at) if state and state.ended_at else ended_at
            ) or now
            window = PopulationTimeWindow(
                started_at_or_after=effective_started_at,
                started_before=effective_ended_at,
            )
            request = FindWorkflowTaskFailuresRequest(
                target_ref=target_ref,
                query=query,
                time_window=window,
                failure_class=failure_class,
                limits=limits,
                sampling_strategy=SamplingStrategy.DETERMINISTIC_HASH,
                sample_seed=state.sample_seed if state else secrets.token_urlsafe(12),
                inspection_offset=0,
                excluded_run_identities=state.inspected_run_ledger if state else (),
                candidate_page_token=state.candidate_page_token if state else None,
                group_by_custom_search_attributes=tuple(group_by_custom_search_attributes or ()),
            )
        except (ValidationError, ValueError, OpsMcpError) as exc:
            if isinstance(exc, ValidationError):
                raise_request_validation_error(exc)
            if isinstance(exc, OpsMcpError):
                raise_service_error(exc)
            raise_invalid_argument(str(exc))
        context = _population_context(root, request.limits)
        try:
            response = await root.services.population.find_workflow_task_failures(context, request)
            preferred = _with_population_continuation(
                response,
                state=state,
                tool="inspect_workflow_task_failures",
                target_ref=target_ref,
                scope=scope,
                sample_seed=request.sample_seed,
                offset=state.inspection_offset if state else 0,
                started_at=request.time_window.started_at_or_after,
                ended_at=request.time_window.started_before,
                confirmed=lambda run: any(
                    finding.code
                    in {
                        "workflow_task_timeout",
                        "workflow_nondeterminism",
                        "workflow_deadlock",
                        "workflow_task_failure",
                    }
                    for finding in run.findings
                ),
                candidate_page_token=request.candidate_page_token,
            )
            return InspectWorkflowTaskFailuresResponse.model_validate(preferred.model_dump())
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def find_workflow_task_failures(
        target_ref: str,
        query: str,
        time_window: PopulationTimeWindow,
        limits: PopulationLimits,
        sampling_strategy: SamplingStrategy,
        sample_seed: str,
        failure_class: FailureClass | None = None,
        group_by_custom_search_attributes: list[str] | None = None,
    ) -> FindWorkflowTaskFailuresResponse:
        """Compatibility alias; prefer inspect_workflow_task_failures."""
        try:
            request = FindWorkflowTaskFailuresRequest(
                target_ref=target_ref,
                query=query,
                time_window=time_window,
                failure_class=failure_class,
                limits=limits,
                sampling_strategy=sampling_strategy,
                sample_seed=sample_seed,
                group_by_custom_search_attributes=tuple(group_by_custom_search_attributes or ()),
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = _population_context(root, request.limits)
        try:
            return await root.services.population.find_workflow_task_failures(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def inspect_activity_queue_dependencies(
        target_ref: Annotated[str, Field(description="Target reference from resolve_target.")],
        query: Annotated[str, Field(description="Raw Temporal Visibility population query.")],
        started_at: Annotated[
            datetime | None, Field(description="Optional inclusive candidate start time.")
        ] = None,
        ended_at: Annotated[
            datetime | None, Field(description="Optional exclusive candidate end time.")
        ] = None,
        activity_task_queues: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Known activity-task queues; omit to discover them from bounded exact-run "
                    "evidence."
                )
            ),
        ] = None,
        expected_duration_seconds: Annotated[
            float | None,
            Field(gt=0, description="Optional operator-supplied normal-duration policy."),
        ] = None,
        group_by_custom_search_attributes: Annotated[
            list[str] | None,
            Field(description="Registered attributes used to group confirmed dependencies."),
        ] = None,
        advanced_options: Annotated[
            PopulationAdvancedOptions | None,
            Field(description="Optional expert overrides on server-owned inspection defaults."),
        ] = None,
        continuation_token: Annotated[
            str | None,
            Field(description="Opaque token returned by a prior call with the same scope."),
        ] = None,
    ) -> InspectActivityQueueDependenciesResponse:
        """Inspect activity dependencies, dispatch, delay, retry, and completeness evidence."""
        now = datetime.now(UTC)
        try:
            if (started_at is None) != (ended_at is None):
                raise ValueError("started_at and ended_at must be supplied together")
            limits = (advanced_options or PopulationAdvancedOptions()).resolved()
            scope = _dependency_scope(
                query,
                started_at,
                ended_at,
                tuple(activity_task_queues or ()),
                expected_duration_seconds,
                tuple(group_by_custom_search_attributes or ()),
                limits,
            )
            state = (
                _CONTINUATIONS.decode(
                    continuation_token,
                    tool="inspect_activity_queue_dependencies",
                    target_ref=target_ref,
                    query=scope,
                )
                if continuation_token
                else None
            )
            effective_started_at = (
                datetime.fromisoformat(state.started_at)
                if state and state.started_at
                else started_at
            ) or datetime(1970, 1, 1, tzinfo=UTC)
            effective_ended_at = (
                datetime.fromisoformat(state.ended_at) if state and state.ended_at else ended_at
            ) or now
            request = FindStuckWorkflowsRequest(
                target_ref=target_ref,
                query=query,
                time_window=PopulationTimeWindow(
                    started_at_or_after=effective_started_at,
                    started_before=effective_ended_at,
                ),
                minimum_open_duration_seconds=expected_duration_seconds,
                limits=limits,
                sampling_strategy=SamplingStrategy.DETERMINISTIC_HASH,
                sample_seed=state.sample_seed if state else secrets.token_urlsafe(12),
                inspection_offset=0,
                excluded_run_identities=state.inspected_run_ledger if state else (),
                candidate_page_token=state.candidate_page_token if state else None,
                require_running_and_duration=False,
                activity_task_queues=tuple(activity_task_queues or ()),
                group_by_custom_search_attributes=tuple(group_by_custom_search_attributes or ()),
            )
        except (ValidationError, ValueError, OpsMcpError) as exc:
            if isinstance(exc, ValidationError):
                raise_request_validation_error(exc)
            if isinstance(exc, OpsMcpError):
                raise_service_error(exc)
            raise_invalid_argument(str(exc))
        context = _population_context(root, request.limits)
        try:
            response = await root.services.population.find_stuck_workflows(context, request)
            preferred = _with_population_continuation(
                response,
                state=state,
                tool="inspect_activity_queue_dependencies",
                target_ref=target_ref,
                scope=scope,
                sample_seed=request.sample_seed,
                offset=request.inspection_offset,
                started_at=request.time_window.started_at_or_after,
                ended_at=request.time_window.started_before,
                confirmed=lambda run: run.activity_queue_membership == "confirmed",
                discovered_activity_task_queues=tuple(
                    snapshot.task_queue
                    for snapshot in response.answer.task_queues
                    if snapshot.task_type == "activity"
                ),
                candidate_page_token=request.candidate_page_token,
            )
            return InspectActivityQueueDependenciesResponse.model_validate(preferred.model_dump())
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def find_stuck_workflows(
        target_ref: str,
        query: str,
        time_window: PopulationTimeWindow,
        minimum_open_duration_seconds: Annotated[float, Field(gt=0)],
        limits: PopulationLimits,
        sampling_strategy: SamplingStrategy,
        sample_seed: str,
        activity_task_queue: str | None = None,
        group_by_custom_search_attributes: list[str] | None = None,
    ) -> FindStuckWorkflowsResponse:
        """Deprecated alias; prefer inspect_activity_queue_dependencies."""
        try:
            request = FindStuckWorkflowsRequest(
                target_ref=target_ref,
                query=query,
                time_window=time_window,
                minimum_open_duration_seconds=minimum_open_duration_seconds,
                limits=limits,
                sampling_strategy=sampling_strategy,
                sample_seed=sample_seed,
                activity_task_queue=activity_task_queue,
                group_by_custom_search_attributes=tuple(group_by_custom_search_attributes or ()),
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = _population_context(root, request.limits)
        try:
            return await root.services.population.find_stuck_workflows(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def analyze_incident_impact(
        target_ref: TargetRefInput,
        query: VisibilityQueryInput,
        started_at: Annotated[
            datetime | None, Field(description="Absolute incident start time.")
        ] = None,
        ended_at: Annotated[
            datetime | None, Field(description="Absolute incident end time.")
        ] = None,
        affected_activity_task_queues: Annotated[
            list[str] | None,
            Field(description="Known affected activity queues; omit for bounded discovery."),
        ] = None,
        expected_duration_seconds: Annotated[
            float | None,
            Field(gt=0, description="Optional operator-supplied normal-duration policy."),
        ] = None,
        group_by_custom_search_attributes: Annotated[
            list[str] | None,
            Field(description="Registered attributes for confirmed-impact grouping."),
        ] = None,
        advanced_options: Annotated[
            PopulationAdvancedOptions | None,
            Field(description="Optional expert overrides on server-owned inspection defaults."),
        ] = None,
        incident_window: Annotated[
            IncidentTimeWindow | None,
            Field(description="Deprecated nested window; prefer started_at and ended_at."),
        ] = None,
        limits: Annotated[
            PopulationLimits | None,
            Field(description="Deprecated execution controls; prefer advanced_options."),
        ] = None,
        sampling_strategy: Annotated[
            SamplingStrategy | None,
            Field(description="Deprecated; the server selects deterministic sampling."),
        ] = None,
        sample_seed: Annotated[
            str | None, Field(description="Deprecated; the server generates the sample seed.")
        ] = None,
        continuation_token: Annotated[
            str | None, Field(description="Opaque token returned by a prior matching call.")
        ] = None,
    ) -> AnalyzeIncidentImpactResponse:
        """Correlate candidate runs with bounded, durable historical incident evidence."""
        try:
            if incident_window is None:
                if started_at is None or ended_at is None:
                    raise ValueError("started_at and ended_at are required")
                incident_window = IncidentTimeWindow(started_at=started_at, ended_at=ended_at)
            elif started_at is not None or ended_at is not None:
                raise ValueError("use started_at/ended_at or deprecated incident_window, not both")
            resolved_limits = limits or (advanced_options or PopulationAdvancedOptions()).resolved()
            scope = _dependency_scope(
                query,
                incident_window.started_at,
                incident_window.ended_at,
                tuple(affected_activity_task_queues or ()),
                expected_duration_seconds,
                tuple(group_by_custom_search_attributes or ()),
                resolved_limits,
            )
            state = (
                _CONTINUATIONS.decode(
                    continuation_token,
                    tool="analyze_incident_impact",
                    target_ref=target_ref,
                    query=scope,
                )
                if continuation_token
                else None
            )
            request = AnalyzeIncidentImpactRequest(
                target_ref=target_ref,
                query=query,
                incident_window=incident_window,
                affected_activity_task_queues=tuple(affected_activity_task_queues or ()),
                expected_duration_seconds=expected_duration_seconds,
                limits=resolved_limits,
                sampling_strategy=(sampling_strategy or SamplingStrategy.DETERMINISTIC_HASH),
                sample_seed=(
                    state.sample_seed if state else sample_seed or secrets.token_urlsafe(12)
                ),
                inspection_offset=0,
                excluded_run_identities=state.inspected_run_ledger if state else (),
                candidate_page_token=state.candidate_page_token if state else None,
                group_by_custom_search_attributes=tuple(group_by_custom_search_attributes or ()),
            )
        except (ValidationError, ValueError, OpsMcpError) as exc:
            if isinstance(exc, ValidationError):
                raise_request_validation_error(exc)
            if isinstance(exc, OpsMcpError):
                raise_service_error(exc)
            raise_invalid_argument(str(exc))
        context = _population_context(root, request.limits)
        try:
            response = await root.services.population.analyze_incident_impact(context, request)
            return _with_population_continuation(
                response,
                state=state,
                tool="analyze_incident_impact",
                target_ref=target_ref,
                scope=scope,
                sample_seed=request.sample_seed,
                offset=request.inspection_offset,
                started_at=request.incident_window.started_at,
                ended_at=request.incident_window.ended_at,
                confirmed=lambda run: run.classification == "confirmed_historical_impact",
                discovered_activity_task_queues=response.answer.affected_activity_task_queues,
                candidate_page_token=request.candidate_page_token,
            )
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def list_schedules(
        target_ref: TargetRefInput,
        name_contains: Annotated[
            str | None, Field(description="Optional case-insensitive schedule ID substring.")
        ] = None,
        shape: Annotated[
            Literal["cron", "interval", "calendar"] | None,
            Field(description="Optional schedule-specification shape filter."),
        ] = None,
        paused: Annotated[bool | None, Field(description="Optional paused-state filter.")] = None,
        page_size: Annotated[
            int, Field(ge=1, le=200, description="Maximum schedule rows returned.")
        ] = 100,
        continuation_token: Annotated[
            str | None, Field(description="Opaque pagination continuation.")
        ] = None,
    ) -> ListSchedulesResponse:
        """Return one bounded schedule page with optional local filters."""
        try:
            request = ListSchedulesRequest(
                target_ref=target_ref,
                name_contains=name_contains,
                shape=shape,
                paused=paused,
                page_size=page_size,
                page_token=continuation_token,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.namespace_reads.list_schedules(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def describe_schedule(
        target_ref: TargetRefInput,
        schedule_id: Annotated[str, Field(description="Exact Temporal Schedule ID.")],
        upcoming_count: Annotated[
            int, Field(ge=0, le=50, description="Number of upcoming action times to calculate.")
        ] = 0,
        upcoming_start: Annotated[
            datetime | None, Field(description="Start of upcoming-time calculation window.")
        ] = None,
        upcoming_end: Annotated[
            datetime | None, Field(description="End of upcoming-time calculation window.")
        ] = None,
    ) -> DescribeScheduleResponse:
        """Describe one schedule and optionally calculate bounded upcoming times."""
        try:
            request = DescribeScheduleRequest(
                target_ref=target_ref,
                schedule_id=schedule_id,
                upcoming_count=upcoming_count,
                upcoming_start=upcoming_start,
                upcoming_end=upcoming_end,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.namespace_reads.describe_schedule(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def list_approved_workflow_queries(
        target_ref: Annotated[
            str,
            Field(
                description="Target reference returned by resolve_target.",
                examples=["orders-prod"],
            ),
        ],
    ) -> ListApprovedWorkflowQueriesResponse:
        """List configured no-argument workflow queries without making a Temporal RPC."""
        try:
            request = ListApprovedWorkflowQueriesRequest(target_ref=target_ref)
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return root.services.namespace_reads.list_approved_workflow_queries(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def query_workflow(
        target_ref: TargetRefInput,
        workflow_id: WorkflowIdInput,
        run_id: RunIdInput,
        query_name: Annotated[
            str, Field(description="Approved no-argument query name from discovery.")
        ],
    ) -> QueryWorkflowResponse:
        """Execute one target-approved no-argument query against an exact workflow run."""
        try:
            request = QueryWorkflowRequest(
                target_ref=target_ref,
                workflow_id=workflow_id,
                run_id=run_id,
                query_name=query_name,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.namespace_reads.query_workflow(context, request)
        except Exception as exc:
            raise_service_error(exc)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def list_batch_operations(
        target_ref: TargetRefInput,
        state: Annotated[
            str | None, Field(description="Optional batch-operation state filter.")
        ] = None,
        job_type: Annotated[
            str | None, Field(description="Optional batch-operation type filter.")
        ] = None,
        page_size: Annotated[
            int, Field(ge=1, le=50, description="Maximum operations returned.")
        ] = 25,
        continuation_token: Annotated[
            str | None, Field(description="Opaque pagination continuation.")
        ] = None,
        include_details: Annotated[
            bool, Field(description="Read bounded per-operation details when true.")
        ] = True,
    ) -> ListBatchOperationsResponse:
        """List a bounded batch-operation page and optional per-operation details."""
        try:
            request = ListBatchOperationsRequest(
                target_ref=target_ref,
                state=state,
                job_type=job_type,
                page_size=page_size,
                page_token=continuation_token,
                include_details=include_details,
            )
        except ValidationError as exc:
            raise_request_validation_error(exc)
        context = root.contexts.create()
        try:
            return await root.services.namespace_reads.list_batch_operations(context, request)
        except Exception as exc:
            raise_service_error(exc)


def _population_context(root: CompositionRoot, limits: PopulationLimits) -> RequestContext:
    history_pages_per_run = (limits.max_history_events_per_run + 249) // 250
    history_pages = limits.max_histories * history_pages_per_run
    temporal_calls = (
        2  # count plus an optional custom-search-attribute schema read
        + limits.max_visibility_pages
        + limits.max_runs
        + history_pages
        + limits.max_task_queues
    )
    return root.contexts.create(
        limits=BasicBudgetLimits(
            temporal_calls=temporal_calls,
            pages=limits.max_visibility_pages + history_pages,
            history_events=limits.max_histories * limits.max_history_events_per_run,
        )
    )


def _population_scope(
    query: str,
    started_at: datetime | None,
    ended_at: datetime | None,
    failure_class: FailureClass | None,
    groups: tuple[str, ...],
    limits: PopulationLimits,
) -> str:
    """Canonical scope signed into a continuation; changed inputs cannot reuse it."""
    return "|".join(
        (
            normalize_query(query),
            started_at.isoformat() if started_at else "server_default_start",
            ended_at.isoformat() if ended_at else "server_default_end",
            failure_class.value if failure_class else "all_failures",
            ",".join(groups),
            limits.model_dump_json(),
        )
    )


def _dependency_scope(
    query: str,
    started_at: datetime | None,
    ended_at: datetime | None,
    queues: tuple[str, ...],
    expected_duration_seconds: float | None,
    groups: tuple[str, ...],
    limits: PopulationLimits,
) -> str:
    return "|".join(
        (
            normalize_query(query),
            started_at.isoformat() if started_at else "server_default_start",
            ended_at.isoformat() if ended_at else "server_default_end",
            ",".join(queues) or "discover_queues",
            (
                str(expected_duration_seconds)
                if expected_duration_seconds is not None
                else "no_duration_policy"
            ),
            ",".join(groups),
            limits.model_dump_json(),
        )
    )


def _with_population_continuation(
    response,
    *,
    state: ContinuationState | None,
    tool: str,
    target_ref: str,
    scope: str,
    sample_seed: str,
    offset: int,
    started_at: datetime,
    ended_at: datetime,
    confirmed,
    discovered_activity_task_queues: tuple[str, ...] = (),
    candidate_page_token: str | None,
):
    inspected_this_call = response.inspection.sample_size
    next_offset = offset + inspected_this_call
    remaining_in_frame = inspected_this_call < response.inspection.candidate_frame_size
    next_candidate_page_token = response.inspection.candidate_next_page_token
    prior_inspected = state.inspected_run_ledger if state else ()
    prior_confirmed = state.confirmed_run_ledger if state else ()
    inspected = tuple(f"{run.workflow_id}\0{run.run_id}" for run in response.answer.runs)
    matched = tuple(
        f"{run.workflow_id}\0{run.run_id}" for run in response.answer.runs if confirmed(run)
    )
    inspected_ledger = tuple(dict.fromkeys((*prior_inspected, *inspected)))
    confirmed_ledger = tuple(dict.fromkeys((*prior_confirmed, *matched)))
    discovered_queues = tuple(
        dict.fromkeys(
            (
                *((state.discovered_activity_task_queues if state else ())),
                *discovered_activity_task_queues,
            )
        )
    )
    public_update = {
        "cumulative_inspected_identities": tuple(
            value.replace("\0", "/") for value in inspected_ledger
        ),
        "cumulative_confirmed_identities": tuple(
            value.replace("\0", "/") for value in confirmed_ledger
        ),
    }
    can_advance_frame = (
        not response.inspection.candidate_collection_complete
        and next_candidate_page_token is not None
    )
    if not remaining_in_frame and not can_advance_frame:
        return response.model_copy(update=public_update)
    token = _CONTINUATIONS.encode(
        ContinuationState(
            tool=tool,
            target_ref=target_ref,
            query=normalize_query(scope),
            sample_seed=sample_seed,
            inspection_offset=next_offset,
            candidate_frame_size=response.inspection.candidate_frame_size,
            candidate_collection_complete=response.inspection.candidate_collection_complete,
            inspected_run_ledger=inspected_ledger,
            confirmed_run_ledger=confirmed_ledger,
            started_at=started_at.isoformat(),
            ended_at=ended_at.isoformat(),
            discovered_activity_task_queues=discovered_queues,
            candidate_page_token=(
                candidate_page_token if remaining_in_frame else next_candidate_page_token
            ),
        )
    )
    return response.model_copy(update={**public_update, "continuation_token": token})
