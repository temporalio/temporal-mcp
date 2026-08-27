"""Authorization-aware services for single-workflow reasoning capabilities."""

from __future__ import annotations

import hashlib
import shlex
from datetime import timedelta
from typing import Protocol

from ops_mcp.analysis.reasoning import (
    AnalysisFinding,
    DiagnosisAnalyzer,
    ResetAnalyzer,
)
from ops_mcp.contracts.namespace_reads import (
    ListApprovedWorkflowQueriesRequest,
    QueryWorkflowAnswer,
    QueryWorkflowRequest,
)
from ops_mcp.contracts.primitives import (
    AnswerConstraints,
    Completeness,
    EvidenceRef,
    EvidenceSource,
    Finding,
    Inspection,
)
from ops_mcp.contracts.reasoning import (
    ActivityReplayImpact,
    CommandPreview,
    CompositeInspection,
    DiagnoseWorkflowRequest,
    DiagnoseWorkflowResponse,
    FailureSummary,
    InspectWorkflowExecutionAnswer,
    InspectWorkflowExecutionRequest,
    InspectWorkflowExecutionResponse,
    PendingActivitySummary,
    PlanWorkflowResetRequest,
    PlanWorkflowResetResponse,
    RecommendedFollowup,
    ReplayImpact,
    ResetPoint,
    ResetRecommendation,
    RetryAssessment,
    TaskQueueSnapshot,
    WorkflowSummary,
)
from ops_mcp.facts.history import (
    ActivityTaskEventAttributes,
    FailureFact,
    HistoryReadFact,
    WorkflowCloseEventAttributes,
    WorkflowTaskEventAttributes,
)
from ops_mcp.facts.task_queues import TaskQueueFact, TaskQueueKind
from ops_mcp.facts.workflows import WorkflowExecutionFact
from ops_mcp.gateways.history import TemporalHistoryGateway
from ops_mcp.gateways.task_queues import TemporalTaskQueueGateway
from ops_mcp.gateways.workflows import TemporalWorkflowGateway
from ops_mcp.runtime.budget import BudgetSnapshot
from ops_mcp.runtime.context import RequestContext
from ops_mcp.services.namespace_reads import NamespaceReadService
from ops_mcp.targets.catalog import TargetCatalog
from ops_mcp.targets.models import ResolvedTarget


class DiagnoseWorkflowService(Protocol):
    async def diagnose_workflow(
        self, context: RequestContext, request: DiagnoseWorkflowRequest
    ) -> DiagnoseWorkflowResponse: ...


class InspectWorkflowExecutionService(Protocol):
    async def inspect_workflow_execution(
        self, context: RequestContext, request: InspectWorkflowExecutionRequest
    ) -> InspectWorkflowExecutionResponse: ...


class PlanWorkflowResetService(Protocol):
    async def plan_workflow_reset(
        self, context: RequestContext, request: PlanWorkflowResetRequest
    ) -> PlanWorkflowResetResponse: ...


class DefaultReasoningService:
    """Orchestrate typed gateways and pure analyzers; contains no SDK/MCP code."""

    def __init__(
        self,
        catalog: TargetCatalog,
        workflow_gateway: TemporalWorkflowGateway,
        history_gateway: TemporalHistoryGateway,
        task_queue_gateway: TemporalTaskQueueGateway,
        *,
        diagnosis_analyzer: DiagnosisAnalyzer | None = None,
        reset_analyzer: ResetAnalyzer | None = None,
        namespace_reads: NamespaceReadService | None = None,
    ) -> None:
        self._catalog = catalog
        self._workflows = workflow_gateway
        self._history = history_gateway
        self._task_queues = task_queue_gateway
        self._diagnosis = diagnosis_analyzer or DiagnosisAnalyzer()
        self._reset = reset_analyzer or ResetAnalyzer()
        self._namespace_reads = namespace_reads

    async def inspect_workflow_execution(
        self, context: RequestContext, request: InspectWorkflowExecutionRequest
    ) -> InspectWorkflowExecutionResponse:
        """Collect the documented single-run evidence set in one read-only call."""
        run_id = request.run_id
        if run_id is None:
            target = self._catalog.authorize(context.principal, request.target_ref)
            resolved = await self._workflows.describe(context, target, request.workflow_id, None)
            run_id = resolved.run_id
        diagnosis = await self.diagnose_workflow(
            context,
            DiagnoseWorkflowRequest(
                target_ref=request.target_ref,
                workflow_id=request.workflow_id,
                run_id=run_id,
                history_event_limit=request.history_event_limit,
                expected_duration_seconds=request.expected_duration_seconds,
            ),
        )
        unavailable: list[str] = []
        collected = [
            "workflow_description",
            "exact_run_id",
            "bounded_workflow_history",
            "pending_activities",
            "workflow_and_activity_task_queue_snapshots",
        ]
        query_name = None
        query_value = None
        warnings = list(diagnosis.warnings)
        if request.include_diagnostic_query:
            if self._namespace_reads is None:
                unavailable.append("diagnostic_query:service_not_configured")
            else:
                discovery = self._namespace_reads.list_approved_workflow_queries(
                    context,
                    ListApprovedWorkflowQueriesRequest(target_ref=request.target_ref),
                )
                names = tuple(item.query_name for item in discovery.answer.approved_queries)
                query_name = next(
                    (name for name in ("__stack_trace", "__enhanced_stack_trace") if name in names),
                    None,
                )
                if query_name is None:
                    unavailable.append("diagnostic_query:no_approved_diagnostic_query")
                else:
                    try:
                        queried = await self._namespace_reads.query_workflow(
                            context,
                            QueryWorkflowRequest(
                                target_ref=request.target_ref,
                                workflow_id=request.workflow_id,
                                run_id=run_id,
                                query_name=query_name,
                            ),
                        )
                    except Exception as exc:
                        unavailable.append(f"diagnostic_query:{type(exc).__name__}")
                    else:
                        if isinstance(queried.answer, QueryWorkflowAnswer):
                            query_value = queried.answer.value
                            collected.append("approved_diagnostic_query")
        else:
            unavailable.append("diagnostic_query:not_requested")

        duration_state = "no_policy"
        if request.expected_duration_seconds is not None:
            collected.append("duration_comparison")
            duration_state = (
                "exceeds_expected_duration"
                if diagnosis.workflow.observed_duration_seconds > request.expected_duration_seconds
                else "within_expected_duration"
            )
        else:
            unavailable.append("duration_comparison:no_policy_supplied")
        if diagnosis.history_inspection.completion is not Completeness.COMPLETE:
            unavailable.append("complete_workflow_history:bounded_or_truncated")

        expected_queue_keys = {(diagnosis.workflow.task_queue, "workflow")}
        expected_queue_keys.update(
            (activity.activity_task_queue, "activity")
            for activity in diagnosis.pending_activities
            if activity.activity_task_queue
        )
        observed_queue_keys = {
            (queue.task_queue, queue.task_type) for queue in diagnosis.task_queues
        }
        for queue_name, queue_type in sorted(expected_queue_keys - observed_queue_keys):
            unavailable.append(f"task_queue_snapshot:{queue_type}:{queue_name}")
        wait_state = "no_wait_observed"
        if diagnosis.pending_activities:
            wait_state = "pending_activity"
        elif any(item.code == "waiting_on_timer" for item in diagnosis.findings):
            wait_state = "durable_wait"
        elif diagnosis.workflow.pending_workflow_task:
            wait_state = "pending_workflow_task"
        relevant = tuple(
            item
            for item in diagnosis.task_queues
            if (item.task_type == "activity" and diagnosis.pending_activities)
            or (item.task_type == "workflow" and diagnosis.workflow.pending_workflow_task)
        )
        if not diagnosis.pending_activities and not diagnosis.workflow.pending_workflow_task:
            dispatch_state = "no_pending_dispatch"
        elif any(item.poller_count == 0 for item in relevant):
            dispatch_state = "dispatch_blocked_now"
        elif relevant:
            dispatch_state = "dispatch_available_now"
        else:
            dispatch_state = "unknown"
        failures = tuple(
            item
            for item in diagnosis.findings
            if any(word in item.code for word in ("fail", "timeout", "nondetermin", "deadlock"))
        )
        retry_findings = tuple(item for item in diagnosis.findings if "retry" in item.code)
        supported = [
            f"duration_state={duration_state} for exact run {request.workflow_id}/{run_id}",
            f"workflow_wait_state={wait_state} for exact run {request.workflow_id}/{run_id}",
            f"current_dispatch_state={dispatch_state} for exact run {request.workflow_id}/{run_id}",
        ]
        unsupported = [
            "current queue state does not establish historical outage duration",
            "Temporal timing evidence alone does not establish root cause",
        ]
        completion = diagnosis.history_inspection.completion.value
        if completion == "unknown":
            completion = "partial"
        return InspectWorkflowExecutionResponse(
            target_ref=diagnosis.target_ref,
            answer=InspectWorkflowExecutionAnswer(
                workflow=diagnosis.workflow,
                duration_state=duration_state,
                progress_state=(
                    "running" if diagnosis.workflow.status.casefold() == "running" else "terminal"
                ),
                workflow_wait_state=wait_state,
                current_dispatch_state=dispatch_state,
                pending_activities=diagnosis.pending_activities,
                task_queues=diagnosis.task_queues,
                failure_findings=failures,
                retry_findings=retry_findings,
                diagnostic_query_name=query_name,
                diagnostic_query_value=query_value,
            ),
            evidence_collected=tuple(collected),
            evidence_unavailable=tuple(unavailable),
            supported_conclusions=tuple(supported),
            unsupported_conclusions=tuple(unsupported),
            inspection=CompositeInspection(
                completion=completion,
                candidate_count=1,
                runs_inspected=1,
                histories_inspected=1,
                inspection_failures=sum(
                    item.startswith(("diagnostic_query:", "task_queue_snapshot:"))
                    and not item.endswith(("not_requested", "no_approved_diagnostic_query"))
                    for item in unavailable
                ),
            ),
            warnings=tuple(warnings),
            observed_at=diagnosis.workflow.observed_at or context.budget.deadline,
            recommended_followups=(
                RecommendedFollowup(
                    intent="recheck_exact_run",
                    tool="inspect_workflow_execution",
                ),
            ),
            population_analysis_required=False,
            answer_constraints=AnswerConstraints(
                may_claim=("the exact run's observed wait, failure, and current dispatch state",),
                must_not_claim=(
                    "population blast radius unless the operator explicitly requests it",
                    "historical queue health from a current queue snapshot",
                ),
            ),
        )

    async def diagnose_workflow(
        self, context: RequestContext, request: DiagnoseWorkflowRequest
    ) -> DiagnoseWorkflowResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        workflow = await self._workflows.describe(
            context, target, request.workflow_id, request.run_id
        )
        history = await self._history.read(
            context,
            target,
            request.workflow_id,
            request.run_id,
            page_size=min(250, request.history_event_limit),
            max_events=request.history_event_limit,
        )
        queues = await self._diagnostic_queues(context, target, workflow, history)
        analysis = self._diagnosis.analyze(
            workflow,
            history,
            queues,
            expected_duration_seconds=request.expected_duration_seconds,
        )
        completion = Completeness.COMPLETE if history.complete else Completeness.TRUNCATED
        warnings = (
            ()
            if history.complete
            else ("History is truncated; findings apply only to inspected events.",)
        )
        response = DiagnoseWorkflowResponse(
            target_ref=target.target_ref,
            workflow=_workflow_summary(workflow, request.expected_duration_seconds),
            pending_activities=tuple(
                PendingActivitySummary(
                    activity_id=item.activity_id,
                    activity_type=item.activity_type,
                    state=item.state,
                    attempt=item.attempt,
                    maximum_attempts=item.maximum_attempts,
                    last_failure=_failure_text(item.last_failure),
                    activity_task_queue=item.activity_task_queue,
                    last_heartbeat_time=item.last_heartbeat_time,
                    heartbeat_details=_decoded_payloads(item.heartbeat_details),
                )
                for item in workflow.pending_activities
            ),
            failures=_failure_summaries(history),
            task_queues=tuple(_queue_snapshot(item) for item in queues),
            findings=tuple(_public_finding(item) for item in analysis.findings),
            retry_assessment=RetryAssessment(
                assessment=analysis.retry.assessment,
                rationale=analysis.retry.rationale,
                missing_evidence=analysis.retry.missing_evidence,
            ),
            history_inspection=_inspection(
                context,
                before,
                completion,
                events_read=history.events_scanned,
                limit=request.history_event_limit,
            ),
            warnings=warnings,
        )
        return _charge_output(context, response)

    async def plan_workflow_reset(
        self, context: RequestContext, request: PlanWorkflowResetRequest
    ) -> PlanWorkflowResetResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        workflow = await self._workflows.describe(
            context, target, request.workflow_id, request.run_id
        )
        history = await self._history.read(
            context,
            target,
            request.workflow_id,
            request.run_id,
            page_size=250,
        )
        complete = history.complete and history.events_scanned >= workflow.history_length
        analysis = self._reset.analyze(workflow, history, complete=complete)
        point_by_id = {point.event_id: point for point in analysis.points}
        recommendation = None
        preview = None
        plan_id = _plan_id(workflow, history, analysis.recommended_event_id)
        if analysis.recommended_event_id is not None:
            point = point_by_id[analysis.recommended_event_id]
            recommendation = ResetRecommendation(
                event_id=point.event_id,
                rationale=(
                    f"Replay begins at {point.label}; topology is supported, while external "
                    "side-effect safety still requires operator review."
                ),
            )
            reason = _reset_reason(plan_id, request.operator_rationale, point.label)
            preview = CommandPreview(command=_reset_command(request, point.event_id, reason))

        completion = Completeness.COMPLETE if complete else Completeness.TRUNCATED
        warnings = list(analysis.warnings)
        if history.complete and not complete:
            warnings.append("history_changed_or_did_not_match_describe_snapshot")
        history_inspection = _inspection(
            context,
            before,
            completion,
            events_read=history.events_scanned,
        )
        evidence_collected = [
            "exact_run_description",
            "exact_run_complete_history" if complete else "bounded_exact_run_history",
            "valid_reset_points",
            "replay_impact",
            "safety_findings",
            "run_and_history_pin",
            "freshness_guidance",
        ]
        evidence_unavailable = [] if complete else ["complete_history"]
        if preview is not None:
            evidence_collected.append("non_executed_command_preview")
        else:
            evidence_unavailable.append(
                "non_executed_command_preview:no_safe_unambiguous_recommendation"
            )
        response = PlanWorkflowResetResponse(
            target_ref=target.target_ref,
            workflow_id=workflow.workflow_id,
            run_id=workflow.run_id,
            plan_id=plan_id,
            observed_history_length=history.events_scanned,
            observed_at=history.observed_at,
            recheck_after=history.observed_at.replace(microsecond=0) + timedelta(minutes=15),
            valid_reset_points=tuple(
                ResetPoint(
                    event_id=point.event_id,
                    event_type=point.event_type,
                    label=point.label,
                    replay_impact=ReplayImpact(
                        completed_activities_preserved=tuple(
                            ActivityReplayImpact(
                                activity_id=item.activity_id,
                                activity_type=item.activity_type,
                            )
                            for item in point.completed_activities_preserved
                        ),
                        activities_replayed=tuple(
                            ActivityReplayImpact(
                                activity_id=item.activity_id,
                                activity_type=item.activity_type,
                            )
                            for item in point.activities_replayed
                        ),
                        signals_reapplied=point.signals_reapplied,
                        updates_reapplied=point.updates_reapplied,
                    ),
                    safety_findings=tuple(_public_finding(item) for item in point.safety_findings),
                    recommendable=point.recommendable,
                )
                for point in analysis.points
            ),
            safety_findings=tuple(_public_finding(item) for item in analysis.safety_findings),
            recommendation=recommendation,
            command_preview=preview,
            operator_rationale=request.operator_rationale,
            history_inspection=history_inspection,
            inspection=CompositeInspection(
                completion=("complete" if complete else "truncated"),
                candidate_count=1,
                runs_inspected=1,
                histories_inspected=1,
                inspection_failures=0,
            ),
            warnings=tuple(dict.fromkeys(warnings)),
            evidence_collected=tuple(evidence_collected),
            evidence_unavailable=tuple(evidence_unavailable),
            supported_conclusions=(
                f"the preview for exact run {workflow.workflow_id}/"
                f"{workflow.run_id} was not executed",
                f"the plan for exact run {workflow.workflow_id}/"
                f"{workflow.run_id} must be rechecked if history changes",
            ),
            unsupported_conclusions=(
                "plan freshness guidance is advisory, not mechanically enforced",
                "external side-effect safety cannot be proven from Temporal history alone",
            ),
            answer_constraints=AnswerConstraints(
                may_claim=(
                    "the preview is pinned to the returned exact run and observed history",
                    "the preview was not executed",
                ),
                must_not_claim=(
                    "the failed dependency has recovered",
                    "external side effects are safe to repeat",
                    "the advisory recheck_after is mechanical plan expiration",
                ),
            ),
        )
        return _charge_output(context, response)

    async def _diagnostic_queues(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        workflow: WorkflowExecutionFact,
        history: HistoryReadFact,
    ) -> tuple[TaskQueueFact, ...]:
        del history
        queue_keys: set[tuple[str, TaskQueueKind]] = {(workflow.task_queue, TaskQueueKind.WORKFLOW)}
        queue_keys.update(
            (item.activity_task_queue, TaskQueueKind.ACTIVITY)
            for item in workflow.pending_activities
            if item.activity_task_queue
        )
        snapshots = []
        for queue, kind in sorted(queue_keys, key=lambda item: (item[0], item[1].value)):
            snapshots.append(await self._task_queues.describe(context, target, queue, kind))
        return tuple(snapshots)


def _workflow_summary(
    workflow: WorkflowExecutionFact, expected_duration_seconds: float | None
) -> WorkflowSummary:
    observed_at = workflow.observed_at or workflow.start_time
    return WorkflowSummary(
        workflow_id=workflow.workflow_id,
        run_id=workflow.run_id,
        workflow_type=workflow.workflow_type,
        status=workflow.status,
        task_queue=workflow.task_queue,
        history_length=workflow.history_length,
        observed_at=workflow.observed_at,
        observed_duration_seconds=max(0.0, (observed_at - workflow.start_time).total_seconds()),
        expected_duration_seconds=expected_duration_seconds,
        pending_workflow_task=workflow.pending_workflow_task is not None,
    )


def _decoded_payloads(value):
    from ops_mcp.contracts.workflow_reads import DecodedPayloadsOutput

    return DecodedPayloadsOutput(
        decode_status=value.decode_status,
        values=value.values,
    )


def _failure_summaries(history: HistoryReadFact) -> tuple[FailureSummary, ...]:
    scheduled: dict[int, ActivityTaskEventAttributes] = {}
    started: dict[int, ActivityTaskEventAttributes] = {}
    result: list[FailureSummary] = []
    for event in history.events:
        attrs = event.attributes
        if isinstance(attrs, ActivityTaskEventAttributes):
            if attrs.phase == "scheduled":
                scheduled[event.event_id] = attrs
            elif attrs.phase == "started":
                started[event.event_id] = attrs
            elif attrs.phase in {"failed", "timed_out"}:
                schedule = scheduled.get(attrs.scheduled_event_id)
                start = started.get(attrs.started_event_id)
                result.append(
                    FailureSummary(
                        event_id=event.event_id,
                        event_type=event.event_type,
                        activity_id=attrs.activity_id
                        or (schedule.activity_id if schedule else None),
                        activity_type=attrs.activity_type
                        or (schedule.activity_type if schedule else None),
                        attempt=attrs.attempt or (start.attempt if start else None),
                        retry_state=attrs.retry_state,
                        messages=_failure_messages(attrs.failure),
                        application_type=_failure_type(attrs.failure),
                    )
                )
        elif isinstance(attrs, WorkflowTaskEventAttributes) and attrs.phase in {
            "failed",
            "timed_out",
        }:
            result.append(
                FailureSummary(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    messages=_failure_messages(attrs.failure),
                    application_type=_failure_type(attrs.failure),
                )
            )
        elif isinstance(attrs, WorkflowCloseEventAttributes) and attrs.state != "completed":
            result.append(
                FailureSummary(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    retry_state=attrs.retry_state,
                    messages=_failure_messages(attrs.failure),
                    application_type=_failure_type(attrs.failure),
                )
            )
    return tuple(result)


def _failure_messages(failure: FailureFact | None) -> tuple[str, ...]:
    result: list[str] = []
    while failure is not None:
        if failure.message and failure.message not in result:
            result.append(failure.message)
        failure = failure.cause
    return tuple(result)


def _failure_type(failure: FailureFact | None) -> str | None:
    while failure is not None:
        if failure.application_type:
            return failure.application_type
        failure = failure.cause
    return None


def _failure_text(failure: FailureFact | None) -> str | None:
    values = _failure_messages(failure)
    return ": ".join(values) if values else None


def _queue_snapshot(queue: TaskQueueFact) -> TaskQueueSnapshot:
    return TaskQueueSnapshot(
        task_queue=queue.task_queue,
        task_type=queue.task_type.value,
        poller_count=len(queue.pollers),
        approximate_backlog_count=queue.approximate_backlog_count,
        approximate_backlog_age_seconds=queue.approximate_backlog_age_seconds,
        observed_at=queue.observed_at,
    )


def _public_finding(finding: AnalysisFinding) -> Finding:
    return Finding(
        code=finding.code,
        summary=finding.summary,
        confidence=finding.confidence,
        evidence=tuple(_evidence(value) for value in finding.evidence),
        counterevidence=tuple(_evidence(value) for value in finding.counterevidence),
        caveats=finding.caveats,
    )


def _evidence(value: str) -> EvidenceRef:
    source, _, reference = value.partition(":")
    mapped = {
        "history": EvidenceSource.HISTORY_EVENT,
        "signal": EvidenceSource.HISTORY_EVENT,
        "task_queue": EvidenceSource.TASK_QUEUE_SNAPSHOT,
        "workflow": EvidenceSource.WORKFLOW_DESCRIPTION,
    }.get(source, EvidenceSource.HISTORY_EVENT)
    return EvidenceRef(source=mapped, reference=reference or value)


def _inspection(
    context: RequestContext,
    before: BudgetSnapshot,
    completion: Completeness,
    *,
    events_read: int | None = None,
    limit: int | None = None,
) -> Inspection:
    after = context.budget.snapshot()
    return Inspection(
        completion=completion,
        temporal_calls=after.temporal_calls - before.temporal_calls,
        pages_read=after.pages_read - before.pages_read,
        events_read=events_read,
        runs_inspected=1,
        limit=limit,
    )


def _plan_id(
    workflow: WorkflowExecutionFact,
    history: HistoryReadFact,
    recommended_event_id: int | None,
) -> str:
    latest = max((event.event_id for event in history.events), default=0)
    payload = f"{workflow.workflow_id}:{workflow.run_id}:{latest}:{recommended_event_id}"
    return f"rp-{hashlib.sha256(payload.encode()).hexdigest()[:8]}"


def _reset_reason(plan_id: str, rationale: str | None, label: str) -> str:
    context = " ".join(rationale.split()) if rationale else f"resume {label}"
    return f"plan {plan_id}: {context}"


def _reset_command(request: PlanWorkflowResetRequest, event_id: int, reason: str) -> str:
    return " ".join(
        (
            "temporal workflow reset",
            "--workflow-id",
            shlex.quote(request.workflow_id),
            "--run-id",
            shlex.quote(request.run_id),
            "--event-id",
            str(event_id),
            "--reason",
            shlex.quote(reason),
        )
    )


def _charge_output(context: RequestContext, response: object) -> object:
    serialized = response.model_dump_json()  # type: ignore[attr-defined]
    context.budget.consume_output_bytes(len(serialized.encode("utf-8")))
    return response
