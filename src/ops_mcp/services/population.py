"""Bounded population-diagnostic services."""

from __future__ import annotations

import base64
import binascii
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol, TypeVar

from ops_mcp.analysis.incident import classify_incident_impact
from ops_mcp.analysis.population import (
    PopulationAnalysisFinding,
    classify_progress_evidence,
    classify_workflow_task_failures,
)
from ops_mcp.contracts.incident import (
    AnalyzeIncidentImpactRequest,
    AnalyzeIncidentImpactResponse,
    IncidentImpactAnswer,
    IncidentImpactClassification,
    IncidentImpactFinding,
    IncidentImpactGroup,
    IncidentImpactRun,
    IncidentImpactSummary,
    IncidentTimeWindow,
)
from ops_mcp.contracts.population import (
    AffectedPopulationAggregation,
    AffectedPopulationGroup,
    BlastRadiusSummary,
    ConfirmedWorkflowTypeGroup,
    FailureClass,
    FailureClassGroup,
    FindStuckWorkflowsRequest,
    FindStuckWorkflowsResponse,
    FindWorkflowTaskFailuresRequest,
    FindWorkflowTaskFailuresResponse,
    InspectedRun,
    PopulationFinding,
    PopulationInspection,
    PopulationLimits,
    PopulationTimeWindow,
    QueueSnapshot,
    SamplingStrategy,
    StuckWorkflowsAnswer,
    WorkflowTaskFailuresAnswer,
)
from ops_mcp.contracts.primitives import Completeness, Confidence, ContractModel
from ops_mcp.facts.history import ActivityTaskEventAttributes, HistoryReadFact
from ops_mcp.facts.task_queues import TaskQueueFact, TaskQueueKind
from ops_mcp.facts.visibility import SearchAttributeFieldFact, WorkflowVisibilityFact
from ops_mcp.facts.workflows import WorkflowExecutionFact
from ops_mcp.gateways.history import TemporalHistoryGateway
from ops_mcp.gateways.task_queues import TemporalTaskQueueGateway
from ops_mcp.gateways.visibility import VisibilityGateway
from ops_mcp.gateways.workflows import TemporalWorkflowGateway
from ops_mcp.runtime.advanced_budget import (
    AdvancedInspectionBudget,
    AdvancedInspectionLimits,
    FanoutResult,
    bounded_fanout,
    deterministic_sample,
)
from ops_mcp.runtime.budget import BudgetSnapshot
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.services.search_attributes import (
    project_search_attribute,
    validate_custom_search_attributes,
)
from ops_mcp.targets.catalog import TargetCatalog
from ops_mcp.targets.models import ResolvedTarget

R = TypeVar("R")


@dataclass(frozen=True)
class _CandidateCollection:
    rows: tuple[WorkflowVisibilityFact, ...]
    pages: int
    next_page_token: bytes | None
    complete: bool
    stop_reason: str


@dataclass(frozen=True)
class _ActivityDependencyEvidence:
    """Reusable durable evidence collected for dependency and incident composites."""

    population_count: int
    collection: _CandidateCollection
    candidates: tuple[WorkflowVisibilityFact, ...]
    selected: tuple[WorkflowVisibilityFact, ...]
    descriptions: tuple[FanoutResult[WorkflowVisibilityFact, WorkflowExecutionFact], ...]
    description_by_id: dict[str, WorkflowExecutionFact]
    histories: tuple[FanoutResult[WorkflowVisibilityFact, HistoryReadFact], ...]
    history_by_id: dict[str, HistoryReadFact]
    history_errors: dict[str, Exception]
    affected_activity_task_queues: frozenset[str]
    history_limited: bool
    advanced: AdvancedInspectionBudget


class PopulationDiagnosticService(Protocol):
    async def analyze_incident_impact(
        self,
        context: RequestContext,
        request: AnalyzeIncidentImpactRequest,
    ) -> AnalyzeIncidentImpactResponse: ...

    async def find_workflow_task_failures(
        self,
        context: RequestContext,
        request: FindWorkflowTaskFailuresRequest,
    ) -> FindWorkflowTaskFailuresResponse: ...

    async def find_stuck_workflows(
        self,
        context: RequestContext,
        request: FindStuckWorkflowsRequest,
    ) -> FindStuckWorkflowsResponse: ...


class DefaultPopulationDiagnosticService:
    def __init__(
        self,
        catalog: TargetCatalog,
        visibility: VisibilityGateway,
        workflows: TemporalWorkflowGateway,
        history: TemporalHistoryGateway,
        task_queues: TemporalTaskQueueGateway,
    ) -> None:
        self._catalog = catalog
        self._visibility = visibility
        self._workflows = workflows
        self._history = history
        self._task_queues = task_queues

    async def find_workflow_task_failures(
        self,
        context: RequestContext,
        request: FindWorkflowTaskFailuresRequest,
    ) -> FindWorkflowTaskFailuresResponse:
        before = context.budget.snapshot()
        target = self._catalog.authorize(context.principal, request.target_ref)
        custom_schema = await self._custom_aggregation_schema(
            context, target, request.group_by_custom_search_attributes
        )
        advanced = _advanced_budget(request.limits)
        effective_query = _effective_query(request.query, request.time_window)
        population = await self._visibility.count_workflows(context, target, query=effective_query)
        collection = await self._collect_candidates(
            context,
            target,
            effective_query,
            request.limits,
            page_token=_decode_candidate_token(request.candidate_page_token),
        )
        collection = _without_inspected(collection, request.excluded_run_identities)
        advanced.consume_candidates(len(collection.rows))
        ranked = deterministic_sample(
            collection.rows,
            size=min(
                request.inspection_offset + request.limits.max_runs,
                len(collection.rows),
            ),
            seed=request.sample_seed,
            identity=_visibility_identity,
        )
        selected = ranked[request.inspection_offset :]
        advanced.consume_runs(len(selected))
        descriptions = await self._describe_runs(
            context, target, selected, request.limits.max_concurrency
        )
        history_rows = selected[: request.limits.max_histories]
        history_limited = len(history_rows) < len(selected)
        advanced.consume_histories(len(history_rows))
        histories = await self._read_histories(
            context,
            target,
            history_rows,
            max_events=request.limits.max_history_events_per_run,
            concurrency=request.limits.max_concurrency,
            reverse=True,
        )
        description_by_id = _values_by_identity(descriptions)
        history_by_id = _values_by_identity(histories)
        errors = _errors_by_identity((*descriptions, *histories))
        runs: list[InspectedRun] = []
        affected_rows: list[WorkflowVisibilityFact] = []
        groups: Counter[FailureClass] = Counter()
        incomplete_histories = history_limited
        for row in selected:
            identity = _visibility_identity(row)
            history = history_by_id.get(identity)
            findings = classify_workflow_task_failures(history) if history else ()
            description = description_by_id.get(identity)
            if description is not None and description.pending_workflow_task is not None:
                pending = description.pending_workflow_task
                findings = (
                    *findings,
                    PopulationAnalysisFinding(
                        code="server_reported_pending_workflow_task",
                        summary=(
                            "Temporal reported a pending workflow task "
                            f"at attempt {pending.attempt}"
                        ),
                        confidence=Confidence.HIGH,
                        evidence=(f"workflow:{row.workflow_id}:{row.run_id}",),
                        caveats=(
                            "A server-reported pending task is not itself a history-confirmed "
                            "workflow-task failure.",
                        ),
                    ),
                )
            if request.failure_class is not None:
                findings = tuple(
                    finding
                    for finding in findings
                    if finding.failure_class is None
                    or finding.failure_class is request.failure_class
                )
            if history is None and identity not in errors:
                findings = (*findings, _history_not_inspected_finding())
            for failure_class in {
                finding.failure_class for finding in findings if finding.failure_class is not None
            }:
                groups[failure_class] += 1
            if any(finding.failure_class is not None for finding in findings):
                affected_rows.append(row)
            if history is not None and not history.complete:
                incomplete_histories = True
            runs.append(
                _inspected_run(
                    row,
                    description,
                    findings,
                    error=errors.get(identity),
                    observed_at=datetime.now(UTC),
                )
            )
        partial_failures = sum(item.error is not None for item in (*descriptions, *histories))
        completion = _completion(
            frame_has_more=not collection.complete,
            frame_size=len(collection.rows),
            sample_size=len(selected),
            partial_failures=partial_failures,
            incomplete_histories=incomplete_histories,
        )
        warnings = _population_warnings(
            collection=collection,
            partial_failures=partial_failures,
            incomplete_histories=incomplete_histories,
        )
        response = FindWorkflowTaskFailuresResponse(
            target_ref=target.target_ref,
            answer=WorkflowTaskFailuresAnswer(
                effective_query=effective_query,
                runs=tuple(runs),
                failure_groups=tuple(
                    FailureClassGroup(failure_class=key, runs=groups[key])
                    for key in sorted(groups, key=lambda item: item.value)
                ),
                affected_aggregation=_affected_aggregation(
                    tuple(affected_rows),
                    custom_names=request.group_by_custom_search_attributes,
                    custom_schema=custom_schema,
                    basis="observed_workflow_task_failure",
                    inspected_runs=len(history_by_id),
                    complete=(
                        collection.complete
                        and len(selected) == len(collection.rows)
                        and partial_failures == 0
                        and not incomplete_histories
                    ),
                ),
            ),
            inspection=_inspection(
                context.budget.snapshot(),
                before,
                advanced,
                population_count=population.count,
                collection=collection,
                sample_size=len(selected),
                runs_inspected=len(description_by_id),
                histories_inspected=len(history_by_id),
                task_queues_inspected=0,
                strategy=request.sampling_strategy,
                seed=request.sample_seed,
                completion=completion,
                partial_failures=partial_failures,
                limits=request.limits,
                history_direction="reverse_recent",
                history_window_truncated=any(
                    item.next_page_token is not None for item in history_by_id.values()
                ),
            ),
            warnings=warnings,
            evidence_collected=(
                "visibility_population_count",
                "bounded_candidate_frame",
                "recent_workflow_task_history",
                "server_reported_workflow_task_state",
                "workflow_task_failure_causes",
                "worker_build_ids_from_history",
                "successful_build_id_membership_from_history",
                "confirmed_workflow_run_identities",
                "inspection_completeness",
            ),
            evidence_unavailable=(("complete_candidate_frame",) if not collection.complete else ())
            + (("complete_history_for_every_candidate",) if incomplete_histories else ()),
            supported_conclusions=(f"history_confirmed_failure_runs={len(affected_rows)}",),
            unsupported_conclusions=(
                "successful BuildIds membership is not a complete exposure population",
                "sampled findings are not an exact population count",
            ),
        )
        return _charge_output(context, response)

    async def analyze_incident_impact(
        self,
        context: RequestContext,
        request: AnalyzeIncidentImpactRequest,
    ) -> AnalyzeIncidentImpactResponse:
        """Correlate a bounded candidate population with durable historical evidence."""

        before = context.budget.snapshot()
        target = self._catalog.authorize(context.principal, request.target_ref)
        custom_schema = await self._custom_aggregation_schema(
            context, target, request.group_by_custom_search_attributes
        )
        effective_query = _incident_overlap_query(request.query, request.incident_window)
        dependency_evidence = await self._inspect_activity_dependency_evidence(
            context,
            target,
            effective_query,
            limits=request.limits,
            sample_seed=request.sample_seed,
            inspection_offset=request.inspection_offset,
            excluded_run_identities=request.excluded_run_identities,
            candidate_page_token=request.candidate_page_token,
            requested_activity_task_queues=request.affected_activity_task_queues,
        )
        advanced = dependency_evidence.advanced
        collection = dependency_evidence.collection
        selected = dependency_evidence.selected
        history_limited = dependency_evidence.history_limited
        history_by_id = dependency_evidence.history_by_id
        errors = _errors_by_identity(
            (*dependency_evidence.descriptions, *dependency_evidence.histories)
        )
        now = datetime.now(UTC)
        affected_queues = dependency_evidence.affected_activity_task_queues
        runs: list[IncidentImpactRun] = []
        confirmed_rows: list[WorkflowVisibilityFact] = []
        for row in selected:
            identity = _visibility_identity(row)
            history = history_by_id.get(identity)
            if history is None:
                classification = IncidentImpactClassification.UNKNOWN
                findings = ()
            else:
                analysis = classify_incident_impact(
                    row,
                    history,
                    incident_window=request.incident_window,
                    affected_activity_task_queues=affected_queues,
                    expected_duration_seconds=request.expected_duration_seconds,
                    observed_at=now,
                )
                classification = analysis.classification
                findings = tuple(
                    IncidentImpactFinding(
                        code=finding.code,
                        summary=finding.summary,
                        confirms_historical_impact=finding.confirms_historical_impact,
                        evidence=finding.evidence,
                        caveats=finding.caveats,
                    )
                    for finding in analysis.findings
                )
                if classification is IncidentImpactClassification.CONFIRMED:
                    confirmed_rows.append(row)
            runs.append(
                IncidentImpactRun(
                    workflow_id=row.workflow_id,
                    run_id=row.run_id,
                    workflow_type=row.workflow_type,
                    status=row.status or "UNKNOWN",
                    start_time=row.start_time,
                    classification=classification,
                    findings=findings,
                    inspection_error_code=_error_code(errors.get(identity)),
                )
            )

        partial_failures = len(errors)
        incomplete_histories = history_limited or any(
            not history.complete for history in history_by_id.values()
        )
        completion = _completion(
            frame_has_more=not collection.complete,
            frame_size=len(collection.rows),
            sample_size=len(selected),
            partial_failures=partial_failures,
            incomplete_histories=incomplete_histories,
        )
        exact_confirmed_count = (
            collection.complete
            and len(selected) == len(collection.rows)
            and len(history_by_id) == len(selected)
            and partial_failures == 0
            and not incomplete_histories
        )
        inspected = len(history_by_id)
        confirmed = len(confirmed_rows)
        unconfirmed = sum(
            run.classification is IncidentImpactClassification.UNCONFIRMED for run in runs
        )
        unknown = sum(run.classification is IncidentImpactClassification.UNKNOWN for run in runs)
        # Failed attempts are reported separately; uninspected means no attempt was selected.
        uninspected = max(0, len(collection.rows) - len(selected))
        warnings = list(
            _population_warnings(
                collection=collection,
                partial_failures=partial_failures,
                incomplete_histories=incomplete_histories,
            )
        )
        warnings.extend(
            (
                "Current task-queue poller/backlog state is intentionally not used as "
                "historical evidence.",
                "Temporal correlation with an affected queue is evidence of impact, "
                "not proof of incident causation.",
            )
        )
        if not exact_confirmed_count:
            warnings.append(
                "confirmed_historical_impact is a lower bound from bounded inspection, "
                "not an exact population count."
            )
        response = AnalyzeIncidentImpactResponse(
            target_ref=target.target_ref,
            answer=IncidentImpactAnswer(
                effective_query=effective_query,
                incident_window=request.incident_window,
                affected_activity_task_queues=tuple(sorted(affected_queues)),
                runs=tuple(runs),
                summary=IncidentImpactSummary(
                    candidate_frame_size=len(collection.rows),
                    inspected=inspected,
                    confirmed_historical_impact=confirmed,
                    inspected_but_unconfirmed=unconfirmed,
                    unknown_or_truncated=unknown,
                    inspection_failures=partial_failures,
                    uninspected=uninspected,
                    confirmed_affected_lower_bound=confirmed,
                    exact_confirmed_count=exact_confirmed_count,
                    result_scope=(
                        "complete_candidate_census"
                        if exact_confirmed_count
                        else "bounded_inspection"
                    ),
                ),
                confirmed_groups=_incident_groups(
                    tuple(confirmed_rows),
                    custom_names=request.group_by_custom_search_attributes,
                    custom_schema=custom_schema,
                ),
            ),
            inspection=_inspection(
                context.budget.snapshot(),
                before,
                advanced,
                population_count=dependency_evidence.population_count,
                collection=collection,
                sample_size=len(selected),
                runs_inspected=inspected,
                histories_inspected=inspected,
                task_queues_inspected=0,
                strategy=request.sampling_strategy,
                seed=request.sample_seed,
                completion=completion,
                partial_failures=partial_failures,
                limits=request.limits,
                history_direction="forward" if history_by_id else "not_read",
                history_window_truncated=any(
                    item.next_page_token is not None for item in history_by_id.values()
                ),
            ),
            warnings=tuple(warnings),
            evidence_collected=(
                "visibility_candidate_frame",
                "shared_activity_dependency_inspection",
                "pending_activity_queue_membership",
                "bounded_workflow_histories",
                "activity_schedule_start_failure_timing",
                "confirmed_workflow_run_identities",
                "registered_search_attribute_grouping",
            ),
            evidence_unavailable=(
                ("historical_task_queue_poller_state",)
                + (("complete_candidate_census",) if not exact_confirmed_count else ())
                + (
                    ("pending_activity_membership_for_every_candidate",)
                    if any(item.error is not None for item in dependency_evidence.descriptions)
                    else ()
                )
            ),
            supported_conclusions=(f"confirmed_impact_lower_bound={confirmed}",),
            unsupported_conclusions=(
                "current pollers cannot establish historical queue health",
                "workflow names do not establish team ownership or monetary domain",
                "temporal correlation does not prove incident causation",
            ),
        )
        return _charge_output(context, response)

    async def find_stuck_workflows(
        self,
        context: RequestContext,
        request: FindStuckWorkflowsRequest,
    ) -> FindStuckWorkflowsResponse:
        before = context.budget.snapshot()
        target = self._catalog.authorize(context.principal, request.target_ref)
        custom_schema = await self._custom_aggregation_schema(
            context, target, request.group_by_custom_search_attributes
        )
        now = datetime.now(UTC)
        base_query = _effective_query(request.query, request.time_window)
        if request.require_running_and_duration:
            assert request.minimum_open_duration_seconds is not None
            cutoff = now.timestamp() - request.minimum_open_duration_seconds
            cutoff_value = datetime.fromtimestamp(cutoff, tz=UTC).isoformat().replace("+00:00", "Z")
            effective_query = (
                f"({base_query}) AND ExecutionStatus = 'Running' AND StartTime <= '{cutoff_value}'"
            )
        else:
            effective_query = base_query
        dependency_evidence = await self._inspect_activity_dependency_evidence(
            context,
            target,
            effective_query,
            limits=request.limits,
            sample_seed=request.sample_seed,
            inspection_offset=request.inspection_offset,
            excluded_run_identities=request.excluded_run_identities,
            candidate_page_token=request.candidate_page_token,
            requested_activity_task_queues=request.requested_activity_task_queues,
            minimum_open_duration_seconds=(
                request.minimum_open_duration_seconds
                if request.require_running_and_duration
                else None
            ),
        )
        advanced = dependency_evidence.advanced
        collection = dependency_evidence.collection
        candidates = dependency_evidence.candidates
        selected = dependency_evidence.selected
        descriptions = dependency_evidence.descriptions
        history_limited = dependency_evidence.history_limited
        histories = dependency_evidence.histories
        description_by_id = dependency_evidence.description_by_id
        history_by_id = dependency_evidence.history_by_id
        description_errors = _errors_by_identity(descriptions)
        errors = _errors_by_identity((*descriptions, *histories))
        queue_keys_by_id = {
            identity: _run_queue_keys(description, history_by_id.get(identity))
            for identity, description in description_by_id.items()
        }
        queue_keys = tuple(
            sorted(
                {key for keys in queue_keys_by_id.values() for key in keys},
                key=lambda item: (item[1].value, item[0]),
            )
        )
        requested_activity_queues = request.requested_activity_task_queues
        if not requested_activity_queues:
            requested_activity_queues = tuple(
                sorted(
                    {
                        name
                        for keys in queue_keys_by_id.values()
                        for name, kind in keys
                        if kind is TaskQueueKind.ACTIVITY
                    }
                )
            )
        if requested_activity_queues:
            queue_keys = tuple(
                sorted(
                    {
                        *queue_keys,
                        *((name, TaskQueueKind.ACTIVITY) for name in requested_activity_queues),
                    },
                    key=lambda item: (item[1].value, item[0]),
                )
            )
        bounded_queue_keys = queue_keys[: request.limits.max_task_queues]
        advanced.consume_task_queues(len(bounded_queue_keys))
        queues = await self._describe_queues(
            context,
            target,
            bounded_queue_keys,
            request.limits.max_concurrency,
        )
        queue_values = tuple(item.value for item in queues if item.value is not None)
        queue_errors = sum(item.error is not None for item in queues)
        membership_by_id = {
            identity: _activity_queue_membership(
                description_by_id.get(identity),
                history_by_id.get(identity),
                requested_activity_queues,
            )
            for identity in (_visibility_identity(row) for row in selected)
        }
        runs = tuple(
            _inspected_run(
                row,
                description_by_id.get(_visibility_identity(row)),
                _progress_findings(
                    _visibility_identity(row),
                    description_by_id,
                    history_by_id,
                    queue_values,
                    queue_keys_by_id.get(_visibility_identity(row), ()),
                    errors,
                    request.minimum_open_duration_seconds,
                ),
                error=errors.get(_visibility_identity(row)),
                observed_at=now,
                activity_queue_membership=membership_by_id[_visibility_identity(row)],
            )
            for row in selected
        )
        incomplete_histories = history_limited or any(
            not item.complete for item in history_by_id.values()
        )
        partial_failures = (
            sum(item.error is not None for item in (*descriptions, *histories)) + queue_errors
        )
        queues_truncated = len(queue_keys) > len(bounded_queue_keys)
        completion = _completion(
            frame_has_more=not collection.complete,
            frame_size=len(candidates),
            sample_size=len(selected),
            partial_failures=partial_failures,
            incomplete_histories=incomplete_histories or queues_truncated,
        )
        warnings = list(
            _population_warnings(
                collection=collection,
                partial_failures=partial_failures,
                incomplete_histories=incomplete_histories,
            )
        )
        if queues_truncated:
            warnings.append("Task-queue inspection was truncated by max_task_queues.")
        warnings.append(
            "No approved diagnostic workflow-query gateway is available; normal signal waits "
            "may remain unknown."
        )
        blast_radius = None
        affected_aggregation = None
        if requested_activity_queues:
            confirmed_rows = tuple(
                row
                for row in selected
                if membership_by_id[_visibility_identity(row)] == "confirmed"
            )
            confirmed_groups = Counter(row.workflow_type for row in confirmed_rows)
            inspection_failures = len(description_errors)
            inspected = len(description_by_id)
            blast_radius = BlastRadiusSummary(
                activity_task_queue=",".join(requested_activity_queues),
                candidate_frame_size=len(candidates),
                inspected=inspected,
                confirmed_affected=len(confirmed_rows),
                inspected_but_unconfirmed=inspected - len(confirmed_rows),
                inspection_failures=inspection_failures,
                uninspected=len(candidates) - len(selected),
                collection_complete=(
                    collection.complete
                    and len(selected) == len(candidates)
                    and inspection_failures == 0
                ),
                confirmed_groups=tuple(
                    ConfirmedWorkflowTypeGroup(
                        workflow_type=name,
                        confirmed_runs=confirmed_groups[name],
                    )
                    for name in sorted(confirmed_groups)
                ),
            )
            affected_aggregation = _affected_aggregation(
                confirmed_rows,
                custom_names=request.group_by_custom_search_attributes,
                custom_schema=custom_schema,
                basis="confirmed_activity_queue_membership",
                inspected_runs=inspected,
                complete=blast_radius.collection_complete,
            )
            warnings.append(
                "Only runs whose pending activity or inspected scheduling history explicitly "
                "names an observed/requested activity queue are counted as confirmed."
            )
        evidence_collected = [
            "visibility_candidate_frame",
            "pending_activity_queue_membership",
            "activity_scheduling_history",
            "retry_and_failure_evidence",
            "confirmed_workflow_run_identities",
        ]
        evidence_unavailable = []
        if queue_values:
            evidence_collected.append("current_task_queue_snapshots")
        elif queue_keys:
            evidence_unavailable.append("current_task_queue_snapshots:read_failed")
        else:
            evidence_unavailable.append("current_task_queue_snapshots:no_observed_queue")
        if not collection.complete:
            evidence_unavailable.append("complete_candidate_frame")
        if incomplete_histories:
            evidence_unavailable.append("complete_history_for_every_candidate")
        if description_errors:
            evidence_unavailable.append("pending_activity_membership_for_every_candidate")
        response = FindStuckWorkflowsResponse(
            target_ref=target.target_ref,
            answer=StuckWorkflowsAnswer(
                effective_query=effective_query,
                runs=runs,
                task_queues=tuple(_queue_snapshot(queue) for queue in queue_values),
                blast_radius=blast_radius,
                affected_aggregation=affected_aggregation,
            ),
            inspection=_inspection(
                context.budget.snapshot(),
                before,
                advanced,
                population_count=dependency_evidence.population_count,
                collection=collection,
                sample_size=len(selected),
                runs_inspected=len(description_by_id),
                histories_inspected=len(history_by_id),
                task_queues_inspected=len(queue_values),
                strategy=request.sampling_strategy,
                seed=request.sample_seed,
                completion=completion,
                partial_failures=partial_failures,
                limits=request.limits,
                history_direction="forward" if history_by_id else "not_read",
                history_window_truncated=any(
                    item.next_page_token is not None for item in history_by_id.values()
                ),
            ),
            warnings=tuple(warnings),
            evidence_collected=tuple(evidence_collected),
            evidence_unavailable=tuple(evidence_unavailable),
            supported_conclusions=(
                "activity dependencies are confirmed only by exact pending/history evidence",
            ),
            unsupported_conclusions=(
                "workflow-task queue membership does not prove an activity dependency",
                "current zero pollers does not prove historical outage duration",
                "duration alone does not prove inability to progress",
            ),
        )
        return _charge_output(context, response)

    async def _custom_aggregation_schema(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        names: tuple[str, ...],
    ) -> dict[str, SearchAttributeFieldFact]:
        if not names:
            return {}
        schema = await self._visibility.get_visibility_schema(context, target)
        return validate_custom_search_attributes(names, schema.fields)

    async def _describe_runs(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        rows: tuple[WorkflowVisibilityFact, ...],
        concurrency: int,
    ) -> tuple[FanoutResult[WorkflowVisibilityFact, WorkflowExecutionFact], ...]:
        async def operation(row: WorkflowVisibilityFact) -> WorkflowExecutionFact:
            return await self._workflows.describe(context, target, row.workflow_id, row.run_id)

        return await bounded_fanout(rows, concurrency=concurrency, operation=operation)

    async def _inspect_activity_dependency_evidence(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        query: str,
        *,
        limits: PopulationLimits,
        sample_seed: str,
        inspection_offset: int,
        excluded_run_identities: tuple[str, ...],
        candidate_page_token: str | None,
        requested_activity_task_queues: tuple[str, ...],
        minimum_open_duration_seconds: float | None = None,
    ) -> _ActivityDependencyEvidence:
        """Collect the durable dependency evidence shared by both intent layers.

        Incident analysis deliberately builds on this collector and adds only
        incident-window classification. Current task-queue snapshots remain an
        optional enrichment owned by inspect_activity_queue_dependencies.
        """

        advanced = _advanced_budget(limits)
        population = await self._visibility.count_workflows(context, target, query=query)
        collection = await self._collect_candidates(
            context,
            target,
            query,
            limits,
            page_token=_decode_candidate_token(candidate_page_token),
        )
        collection = _without_inspected(collection, excluded_run_identities)
        now = datetime.now(UTC)
        candidates = collection.rows
        if minimum_open_duration_seconds is not None:
            candidates = tuple(
                row
                for row in candidates
                if row.status == "RUNNING"
                and (now - row.start_time).total_seconds() >= minimum_open_duration_seconds
            )
        advanced.consume_candidates(len(candidates))
        ranked = deterministic_sample(
            candidates,
            size=min(inspection_offset + limits.max_runs, len(candidates)),
            seed=sample_seed,
            identity=_visibility_identity,
        )
        selected = ranked[inspection_offset:]
        advanced.consume_runs(len(selected))
        descriptions = await self._describe_runs(
            context, target, selected, limits.max_concurrency
        )
        description_by_id = _values_by_identity(descriptions)
        history_rows = selected[: limits.max_histories]
        history_limited = len(history_rows) < len(selected)
        advanced.consume_histories(len(history_rows))
        histories = await self._read_histories(
            context,
            target,
            history_rows,
            max_events=limits.max_history_events_per_run,
            concurrency=limits.max_concurrency,
            reverse=False,
        )
        history_by_id = _values_by_identity(histories)
        affected_queues = frozenset(requested_activity_task_queues)
        if not affected_queues:
            affected_queues = frozenset(
                {
                    activity.activity_task_queue
                    for workflow in description_by_id.values()
                    for activity in workflow.pending_activities
                    if activity.activity_task_queue
                }
                | {
                    attrs.task_queue
                    for history in history_by_id.values()
                    for event in history.events
                    if isinstance((attrs := event.attributes), ActivityTaskEventAttributes)
                    and attrs.phase == "scheduled"
                    and attrs.task_queue
                }
            )
        return _ActivityDependencyEvidence(
            population_count=population.count,
            collection=collection,
            candidates=candidates,
            selected=selected,
            descriptions=descriptions,
            description_by_id=description_by_id,
            histories=histories,
            history_by_id=history_by_id,
            history_errors=_errors_by_identity(histories),
            affected_activity_task_queues=affected_queues,
            history_limited=history_limited,
            advanced=advanced,
        )

    async def _collect_candidates(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        query: str,
        limits: PopulationLimits,
        *,
        page_token: bytes | None = None,
    ) -> _CandidateCollection:
        """Collect a reusable bounded visibility frame before sampling it."""

        rows: list[WorkflowVisibilityFact] = []
        token = page_token
        pages = 0
        while len(rows) < limits.max_candidates and pages < limits.max_visibility_pages:
            previous_token = token
            page = await self._visibility.list_workflows_page(
                context,
                target,
                query=query,
                page_size=min(200, limits.max_candidates - len(rows)),
                page_token=token,
            )
            pages += 1
            rows.extend(page.rows)
            token = page.next_page_token
            if token is None:
                return _CandidateCollection(tuple(rows), pages, None, True, "complete")
            if not page.rows and token == previous_token:
                return _CandidateCollection(tuple(rows), pages, token, False, "stalled_page_token")
        stop_reason = (
            "max_candidates" if len(rows) >= limits.max_candidates else "max_visibility_pages"
        )
        return _CandidateCollection(tuple(rows), pages, token, token is None, stop_reason)

    async def _read_histories(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        rows: tuple[WorkflowVisibilityFact, ...],
        *,
        max_events: int,
        concurrency: int,
        reverse: bool,
    ) -> tuple[FanoutResult[WorkflowVisibilityFact, HistoryReadFact], ...]:
        async def operation(row: WorkflowVisibilityFact) -> HistoryReadFact:
            return await self._history.read(
                context,
                target,
                row.workflow_id,
                row.run_id,
                page_size=min(250, max_events),
                max_events=max_events,
                reverse=reverse,
            )

        return await bounded_fanout(rows, concurrency=concurrency, operation=operation)

    async def _describe_queues(
        self,
        context: RequestContext,
        target: ResolvedTarget,
        keys: tuple[tuple[str, TaskQueueKind], ...],
        concurrency: int,
    ) -> tuple[FanoutResult[tuple[str, TaskQueueKind], TaskQueueFact], ...]:
        async def operation(key: tuple[str, TaskQueueKind]) -> TaskQueueFact:
            return await self._task_queues.describe(context, target, key[0], key[1])

        return await bounded_fanout(keys, concurrency=concurrency, operation=operation)


def _advanced_budget(limits: PopulationLimits) -> AdvancedInspectionBudget:
    return AdvancedInspectionBudget(
        AdvancedInspectionLimits(
            candidates=limits.max_candidates,
            runs=limits.max_runs,
            histories=limits.max_histories,
            task_queues=limits.max_task_queues,
            concurrency=limits.max_concurrency,
        )
    )


def _effective_query(query: str, window: PopulationTimeWindow) -> str:
    start = window.started_at_or_after.astimezone(UTC).isoformat().replace("+00:00", "Z")
    end = window.started_before.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return f"({query.strip()}) AND StartTime >= '{start}' AND StartTime < '{end}'"


def _incident_overlap_query(query: str, window: IncidentTimeWindow) -> str:
    start = window.started_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    end = window.ended_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return (
        f"({query.strip()}) AND StartTime < '{end}' AND "
        f"(CloseTime >= '{start}' OR ExecutionStatus = 'Running')"
    )


def _visibility_identity(row: WorkflowVisibilityFact) -> str:
    return f"{row.workflow_id}\0{row.run_id}"


def _without_inspected(
    collection: _CandidateCollection, excluded: tuple[str, ...]
) -> _CandidateCollection:
    if not excluded:
        return collection
    excluded_set = set(excluded)
    return _CandidateCollection(
        rows=tuple(row for row in collection.rows if _visibility_identity(row) not in excluded_set),
        pages=collection.pages,
        next_page_token=collection.next_page_token,
        complete=collection.complete,
        stop_reason=collection.stop_reason,
    )


def _affected_aggregation(
    rows: tuple[WorkflowVisibilityFact, ...],
    *,
    custom_names: tuple[str, ...],
    custom_schema: dict[str, SearchAttributeFieldFact],
    basis: Literal["observed_workflow_task_failure", "confirmed_activity_queue_membership"],
    inspected_runs: int,
    complete: bool,
) -> AffectedPopulationAggregation:
    grouped: dict[tuple[object, ...], tuple[WorkflowVisibilityFact, int]] = {}
    for row in rows:
        key = (
            row.workflow_type,
            *(row.search_attributes.get(name) for name in custom_names),
        )
        representative, count = grouped.get(key, (row, 0))
        grouped[key] = (representative, count + 1)
    groups = tuple(
        AffectedPopulationGroup(
            workflow_type=row.workflow_type,
            custom_search_attributes=tuple(
                project_search_attribute(name, custom_schema[name], row.search_attributes)
                for name in custom_names
            ),
            confirmed_runs=count,
        )
        for _, (row, count) in sorted(grouped.items(), key=lambda item: repr(item[0]))
    )
    return AffectedPopulationAggregation(
        basis=basis,
        scope="complete_query_population" if complete else "bounded_inspection",
        dimensions=("workflow_type", *custom_names),
        inspected_runs=inspected_runs,
        confirmed_affected_runs=len(rows),
        groups=groups,
    )


def _incident_groups(
    rows: tuple[WorkflowVisibilityFact, ...],
    *,
    custom_names: tuple[str, ...],
    custom_schema: dict[str, SearchAttributeFieldFact],
) -> tuple[IncidentImpactGroup, ...]:
    """Group only history-confirmed rows; visibility candidates are never promoted."""

    grouped: dict[tuple[object, ...], tuple[WorkflowVisibilityFact, int]] = {}
    for row in rows:
        key = (
            row.workflow_type,
            *(row.search_attributes.get(name) for name in custom_names),
        )
        representative, count = grouped.get(key, (row, 0))
        grouped[key] = (representative, count + 1)
    return tuple(
        IncidentImpactGroup(
            workflow_type=row.workflow_type,
            custom_search_attributes=tuple(
                project_search_attribute(name, custom_schema[name], row.search_attributes)
                for name in custom_names
            ),
            confirmed_runs=count,
        )
        for _, (row, count) in sorted(grouped.items(), key=lambda item: repr(item[0]))
    )


def _values_by_identity(
    results: tuple[FanoutResult[WorkflowVisibilityFact, R], ...],
) -> dict[str, R]:
    return {
        _visibility_identity(item.item): item.value for item in results if item.value is not None
    }


def _errors_by_identity(
    results: tuple[FanoutResult[WorkflowVisibilityFact, object], ...],
) -> dict[str, Exception]:
    return {
        _visibility_identity(item.item): item.error for item in results if item.error is not None
    }


def _error_code(error: Exception | None) -> str | None:
    if isinstance(error, OpsMcpError):
        return error.code.value
    return "internal_error" if error else None


def _inspected_run(
    row: WorkflowVisibilityFact,
    description: WorkflowExecutionFact | None,
    findings: tuple[PopulationAnalysisFinding, ...],
    *,
    error: Exception | None,
    observed_at: datetime,
    activity_queue_membership: str | None = None,
) -> InspectedRun:
    return InspectedRun(
        workflow_id=row.workflow_id,
        run_id=row.run_id,
        workflow_type=row.workflow_type,
        status=description.status if description else row.status or "UNKNOWN",
        start_time=row.start_time,
        open_duration_seconds=max(0.0, (observed_at - row.start_time).total_seconds()),
        findings=tuple(_public_finding(finding) for finding in findings),
        inspection_error_code=_error_code(error),
        activity_queue_membership=activity_queue_membership,
    )


def _public_finding(finding: PopulationAnalysisFinding) -> PopulationFinding:
    return PopulationFinding(
        code=finding.code,
        summary=finding.summary,
        confidence=finding.confidence,
        evidence=finding.evidence,
        caveats=finding.caveats,
        failure_messages=finding.failure_messages,
        stack_trace_excerpt=finding.stack_trace_excerpt,
        worker_build_id=finding.worker_build_id,
        occurred_at=finding.occurred_at,
    )


def _history_not_inspected_finding() -> PopulationAnalysisFinding:
    return PopulationAnalysisFinding(
        code="history_not_inspected_due_to_limit",
        summary="This run's history was not inspected because the history limit was reached",
        confidence=Confidence.UNKNOWN,
        caveats=("No claim is made about workflow-task failures in this run.",),
    )


def _progress_findings(
    identity: str,
    descriptions: dict[str, WorkflowExecutionFact],
    histories: dict[str, HistoryReadFact],
    queues: tuple[TaskQueueFact, ...],
    queue_keys: tuple[tuple[str, TaskQueueKind], ...],
    errors: dict[str, Exception],
    expected_duration_seconds: float | None,
) -> tuple[PopulationAnalysisFinding, ...]:
    description = descriptions.get(identity)
    if description is None:
        return ()
    history = histories.get(identity)
    scoped_queues = tuple(
        queue for queue in queues if (queue.task_queue, queue.task_type) in queue_keys
    )
    findings = classify_progress_evidence(description, history, scoped_queues)
    observed_at = description.close_time or description.observed_at
    if observed_at is not None and expected_duration_seconds is not None:
        duration = max(0.0, (observed_at - description.start_time).total_seconds())
        if duration > expected_duration_seconds:
            findings = (
                *findings,
                PopulationAnalysisFinding(
                    code="duration_exceeds_policy",
                    summary=(
                        f"Run duration {duration:.0f}s exceeded the supplied "
                        f"{expected_duration_seconds:.0f}s policy"
                    ),
                    confidence=Confidence.HIGH,
                    evidence=(f"workflow:{description.workflow_id}:{description.run_id}",),
                    caveats=(
                        "Duration establishes delay, not inability to make progress or cause.",
                    ),
                ),
            )
    if history is None and identity not in errors:
        return (*findings, _history_not_inspected_finding())
    return findings


def _run_queue_keys(
    description: WorkflowExecutionFact,
    history: HistoryReadFact | None,
) -> tuple[tuple[str, TaskQueueKind], ...]:
    keys = {(description.task_queue, TaskQueueKind.WORKFLOW)}
    keys.update(
        (activity.activity_task_queue, TaskQueueKind.ACTIVITY)
        for activity in description.pending_activities
        if activity.activity_task_queue
    )
    if history is not None:
        keys.update(
            (attrs.task_queue, TaskQueueKind.ACTIVITY)
            for event in history.events
            if isinstance((attrs := event.attributes), ActivityTaskEventAttributes)
            and attrs.phase == "scheduled"
            and attrs.task_queue
        )
    return tuple(sorted(keys, key=lambda item: (item[1].value, item[0])))


def _activity_queue_membership(
    description: WorkflowExecutionFact | None,
    history: HistoryReadFact | None,
    activity_task_queues: tuple[str, ...],
) -> str | None:
    if not activity_task_queues:
        return None
    wanted = set(activity_task_queues)
    if description is None and history is None:
        return "unknown"
    pending_match = description is not None and any(
        activity.activity_task_queue in wanted for activity in description.pending_activities
    )
    history_match = history is not None and any(
        isinstance(event.attributes, ActivityTaskEventAttributes)
        and event.attributes.phase == "scheduled"
        and event.attributes.task_queue in wanted
        for event in history.events
    )
    if pending_match or history_match:
        return "confirmed"
    if history is not None and not history.complete:
        return "unknown"
    return "unconfirmed"


def _queue_snapshot(queue: TaskQueueFact) -> QueueSnapshot:
    return QueueSnapshot(
        task_queue=queue.task_queue,
        task_type=queue.task_type.value,
        poller_count=len(queue.pollers),
        currently_has_pollers=queue.currently_has_pollers,
        approximate_backlog_count=queue.approximate_backlog_count,
        approximate_backlog_age_seconds=queue.approximate_backlog_age_seconds,
        observed_at=queue.observed_at,
    )


def _completion(
    *,
    frame_has_more: bool,
    frame_size: int,
    sample_size: int,
    partial_failures: int,
    incomplete_histories: bool,
) -> Completeness:
    if partial_failures:
        return Completeness.PARTIAL
    if frame_has_more or sample_size < frame_size:
        return Completeness.SAMPLED
    if incomplete_histories:
        return Completeness.TRUNCATED
    return Completeness.COMPLETE


def _population_warnings(
    *, collection: _CandidateCollection, partial_failures: int, incomplete_histories: bool
) -> tuple[str, ...]:
    warnings: list[str] = []
    if not collection.complete:
        warnings.append(
            "The deterministic sample was selected from a bounded, truncated visibility-query "
            "frame. It is not population-representative and must not be extrapolated."
        )
    if partial_failures:
        warnings.append(f"{partial_failures} bounded inspection operation(s) failed.")
    if incomplete_histories:
        warnings.append("One or more histories were truncated by the per-run event limit.")
    return tuple(warnings)


def _inspection(
    after: BudgetSnapshot,
    before: BudgetSnapshot,
    advanced: AdvancedInspectionBudget,
    *,
    population_count: int,
    collection: _CandidateCollection,
    sample_size: int,
    runs_inspected: int,
    histories_inspected: int,
    task_queues_inspected: int,
    strategy: SamplingStrategy,
    seed: str,
    completion: Completeness,
    partial_failures: int,
    limits: PopulationLimits,
    history_direction: Literal["not_read", "forward", "reverse_recent"],
    history_window_truncated: bool,
) -> PopulationInspection:
    snapshot = advanced.snapshot()
    estimate_eligible = (
        collection.complete
        and 0 < sample_size < len(collection.rows)
        and histories_inspected == sample_size
        and partial_failures == 0
        and not history_window_truncated
    )
    if not collection.complete:
        estimate_reason = "truncated_candidate_frame"
    elif sample_size == len(collection.rows):
        estimate_reason = "candidate_frame_census"
    elif not estimate_eligible:
        estimate_reason = "incomplete_sample_inspection"
    else:
        estimate_reason = "complete_frame_uniform_sample"
    return PopulationInspection(
        completion=(Completeness.BUDGET_EXHAUSTED if after.exhausted_dimension else completion),
        temporal_calls=after.temporal_calls - before.temporal_calls,
        pages_read=after.pages_read - before.pages_read,
        history_events_read=after.history_events_read - before.history_events_read,
        population_count=population_count,
        candidate_frame_size=snapshot.candidates,
        candidate_count=snapshot.candidates,
        candidate_frame_has_more=not collection.complete,
        candidate_collection_pages=collection.pages,
        candidate_collection_complete=collection.complete,
        candidate_collection_stop_reason=collection.stop_reason,
        sample_size=sample_size,
        runs_inspected=runs_inspected,
        histories_inspected=histories_inspected,
        task_queues_inspected=task_queues_inspected,
        sampling_strategy=strategy,
        sample_seed=seed,
        sampling_frame=(
            "complete_visibility_query" if collection.complete else "bounded_visibility_query"
        ),
        estimate_eligible=estimate_eligible,
        estimate_eligibility_reason=estimate_reason,
        history_direction=history_direction,
        history_window_truncated=history_window_truncated,
        partial_failures=partial_failures,
        inspection_failures=partial_failures,
        exhausted_dimension=after.exhausted_dimension,
        limits=limits,
        candidate_next_page_token=_encode_candidate_token(collection.next_page_token),
    )


def _encode_candidate_token(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _decode_candidate_token(value: str | None) -> bytes | None:
    if value is None:
        return None
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, binascii.Error) as exc:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "candidate continuation cursor is invalid",
            retryable=False,
        ) from exc


def _charge_output(context: RequestContext, response: ContractModel):
    context.budget.consume_output_bytes(len(response.model_dump_json().encode("utf-8")))
    return response
