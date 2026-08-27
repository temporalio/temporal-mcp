"""Services for schedule, approved-query, and batch-operation reads."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import Protocol, TypeVar

from ops_mcp.contracts.namespace_reads import (
    ApprovedQueryOutput,
    BatchOperationOutput,
    DescribeScheduleAnswer,
    DescribeScheduleRequest,
    DescribeScheduleResponse,
    ListApprovedWorkflowQueriesAnswer,
    ListApprovedWorkflowQueriesRequest,
    ListApprovedWorkflowQueriesResponse,
    ListBatchOperationsAnswer,
    ListBatchOperationsRequest,
    ListBatchOperationsResponse,
    ListSchedulesAnswer,
    ListSchedulesRequest,
    ListSchedulesResponse,
    QueryWorkflowAnswer,
    QueryWorkflowDiscoveryAnswer,
    QueryWorkflowRequest,
    QueryWorkflowResponse,
    ScheduleActionOutput,
    ScheduleSummaryOutput,
)
from ops_mcp.contracts.primitives import Completeness, ContractModel, Inspection
from ops_mcp.facts.namespace_reads import (
    BatchOperationDetailFact,
    BatchOperationSummaryFact,
    ScheduleDescriptionFact,
    ScheduleSummaryFact,
)
from ops_mcp.gateways.namespace_reads import BatchGateway, QueryGateway, ScheduleGateway
from ops_mcp.runtime.budget import BudgetSnapshot
from ops_mcp.runtime.context import RequestContext
from ops_mcp.runtime.errors import ErrorCode, OpsMcpError
from ops_mcp.targets.catalog import TargetCatalog


class QueryApprovalPolicy(Protocol):
    def is_approved(self, target_ref: str, query_name: str) -> bool: ...

    def approved_query_names(self, target_ref: str) -> tuple[str, ...]: ...


@dataclass(frozen=True)
class ConfiguredQueryApprovalPolicy:
    approved_names: dict[str, frozenset[str]]

    def is_approved(self, target_ref: str, query_name: str) -> bool:
        return query_name in self.approved_names.get(target_ref, frozenset())

    def approved_query_names(self, target_ref: str) -> tuple[str, ...]:
        return tuple(sorted(self.approved_names.get(target_ref, frozenset())))


class NamespaceReadService(Protocol):
    def list_approved_workflow_queries(
        self, context: RequestContext, request: ListApprovedWorkflowQueriesRequest
    ) -> ListApprovedWorkflowQueriesResponse: ...

    async def list_schedules(
        self, context: RequestContext, request: ListSchedulesRequest
    ) -> ListSchedulesResponse: ...

    async def describe_schedule(
        self, context: RequestContext, request: DescribeScheduleRequest
    ) -> DescribeScheduleResponse: ...

    async def query_workflow(
        self, context: RequestContext, request: QueryWorkflowRequest
    ) -> QueryWorkflowResponse: ...

    async def list_batch_operations(
        self, context: RequestContext, request: ListBatchOperationsRequest
    ) -> ListBatchOperationsResponse: ...


class DefaultNamespaceReadService:
    def __init__(
        self,
        catalog: TargetCatalog,
        schedules: ScheduleGateway,
        queries: QueryGateway,
        batches: BatchGateway,
        query_policy: QueryApprovalPolicy,
    ) -> None:
        self._catalog = catalog
        self._schedules = schedules
        self._queries = queries
        self._batches = batches
        self._query_policy = query_policy

    def list_approved_workflow_queries(
        self, context: RequestContext, request: ListApprovedWorkflowQueriesRequest
    ) -> ListApprovedWorkflowQueriesResponse:
        """Return target configuration only; this deliberately makes no Temporal RPC."""
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        response = ListApprovedWorkflowQueriesResponse(
            target_ref=target.target_ref,
            answer=ListApprovedWorkflowQueriesAnswer(
                approved_queries=tuple(
                    _approved_query_output(name)
                    for name in self._query_policy.approved_query_names(target.target_ref)
                )
            ),
            inspection=_inspection(context, before, Completeness.COMPLETE),
            warnings=(
                "Approved names are configuration, not proof that a particular workflow "
                "worker supports them.",
            ),
        )
        return _charge_output(context, response)

    async def list_schedules(
        self, context: RequestContext, request: ListSchedulesRequest
    ) -> ListSchedulesResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        rows, next_token = await self._schedules.list_page(
            context,
            target,
            page_size=request.page_size,
            page_token=_decode_token(request.page_token),
        )
        selected = tuple(row for row in rows if _schedule_matches(row, request))
        unevaluable = any(_schedule_filter_is_unknown(row, request) for row in rows)
        completion = (
            Completeness.TRUNCATED
            if next_token
            else Completeness.UNKNOWN
            if unevaluable
            else Completeness.COMPLETE
        )
        response = ListSchedulesResponse(
            target_ref=target.target_ref,
            answer=ListSchedulesAnswer(
                schedules=tuple(_schedule_summary(row) for row in selected),
                server_rows_scanned=len(rows),
                returned=len(selected),
                continuation_token=_encode_token(next_token),
            ),
            inspection=_inspection(context, before, completion, limit=request.page_size),
            warnings=(
                (("Additional server schedule rows were not scanned.",) if next_token else ())
                + (
                    ("Some schedule rows lacked fields required by the local filters.",)
                    if unevaluable
                    else ()
                )
            ),
        )
        return _charge_output(context, response)

    async def describe_schedule(
        self, context: RequestContext, request: DescribeScheduleRequest
    ) -> DescribeScheduleResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        fact = await self._schedules.describe(
            context,
            target,
            request.schedule_id,
            upcoming_start=request.upcoming_start,
            upcoming_end=request.upcoming_end,
            upcoming_count=request.upcoming_count,
        )
        response = DescribeScheduleResponse(
            target_ref=target.target_ref,
            answer=_schedule_description(fact),
            inspection=_inspection(context, before, Completeness.COMPLETE),
        )
        return _charge_output(context, response)

    async def query_workflow(
        self, context: RequestContext, request: QueryWorkflowRequest
    ) -> QueryWorkflowResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        if request.query_name is None:
            response = QueryWorkflowResponse(
                target_ref=target.target_ref,
                answer=QueryWorkflowDiscoveryAnswer(
                    workflow_id=request.workflow_id,
                    run_id=request.run_id,
                    approved_queries=tuple(
                        _approved_query_output(name)
                        for name in self._query_policy.approved_query_names(target.target_ref)
                    ),
                ),
                inspection=_inspection(context, before, Completeness.COMPLETE),
                warnings=(
                    "Approved names are configuration, not proof that this run's worker "
                    "supports them; executing a query requires an available workflow worker.",
                ),
            )
            return _charge_output(context, response)
        if not self._query_policy.is_approved(target.target_ref, request.query_name):
            raise OpsMcpError(
                ErrorCode.QUERY_NOT_APPROVED,
                "The query name is not approved for this target",
                retryable=False,
            )
        fact = await self._queries.query(
            context,
            target,
            request.workflow_id,
            request.run_id,
            request.query_name,
        )
        completion = (
            Completeness.COMPLETE if fact.decode_status == "decoded" else Completeness.UNKNOWN
        )
        response = QueryWorkflowResponse(
            target_ref=target.target_ref,
            answer=QueryWorkflowAnswer(
                workflow_id=fact.workflow_id,
                run_id=fact.run_id,
                query_name=fact.query_name,
                decode_status=fact.decode_status,
                value=fact.value,
                observed_at=fact.observed_at,
            ),
            inspection=_inspection(context, before, completion),
            warnings=(
                ("The query result could not be decoded.",)
                if fact.decode_status == "decode_failed"
                else ()
            ),
        )
        return _charge_output(context, response)

    async def list_batch_operations(
        self, context: RequestContext, request: ListBatchOperationsRequest
    ) -> ListBatchOperationsResponse:
        target = self._catalog.authorize(context.principal, request.target_ref)
        before = context.budget.snapshot()
        page = await self._batches.list_page(
            context,
            target,
            page_size=request.page_size,
            page_token=_decode_token(request.page_token),
        )
        selected = tuple(row for row in page.operations if _batch_matches(row, request))
        outputs: list[BatchOperationOutput] = []
        detail_budget_exhausted = False
        detail_unknown = False
        for row in selected:
            if not request.include_details:
                outputs.append(_batch_output(row, None, "not_requested"))
                continue
            if detail_budget_exhausted:
                outputs.append(_batch_output(row, None, "budget_exhausted"))
                continue
            try:
                detail = await self._batches.describe(context, target, row.job_id)
            except OpsMcpError as exc:
                if exc.code is ErrorCode.BUDGET_EXHAUSTED:
                    detail_budget_exhausted = True
                    outputs.append(_batch_output(row, None, "budget_exhausted"))
                else:
                    detail_unknown = True
                    outputs.append(_batch_output(row, None, "unknown"))
            else:
                outputs.append(_batch_output(row, detail, "complete"))

        if detail_budget_exhausted:
            completion = Completeness.BUDGET_EXHAUSTED
        elif page.next_page_token:
            completion = Completeness.TRUNCATED
        elif detail_unknown:
            completion = Completeness.UNKNOWN
        else:
            completion = Completeness.COMPLETE
        warnings = ()
        if detail_budget_exhausted:
            warnings = ("The detail call budget was exhausted; some details are incomplete.",)
        elif detail_unknown:
            warnings = ("Some batch-operation details could not be read.",)
        response = ListBatchOperationsResponse(
            target_ref=target.target_ref,
            answer=ListBatchOperationsAnswer(
                operations=tuple(outputs),
                returned=len(outputs),
                continuation_token=_encode_token(page.next_page_token),
            ),
            inspection=_inspection(context, before, completion, limit=request.page_size),
            warnings=warnings,
        )
        return _charge_output(context, response)


def _approved_query_output(query_name: str) -> ApprovedQueryOutput:
    known = {
        "__stack_trace": (
            "Python SDK built-in plain-text workflow stack trace",
            "sensitive",
        ),
        "__enhanced_stack_trace": (
            "Python SDK built-in enhanced workflow stack trace",
            "sensitive",
        ),
    }
    description, sensitivity = known.get(
        query_name,
        ("Target-approved custom no-argument workflow query", "ordinary"),
    )
    return ApprovedQueryOutput(
        query_name=query_name,
        description=description,
        sensitivity=sensitivity,
    )


def _schedule_matches(row: ScheduleSummaryFact, request: ListSchedulesRequest) -> bool:
    return (
        (
            request.name_contains is None
            or request.name_contains.casefold() in row.schedule_id.casefold()
        )
        and (request.shape is None or request.shape in row.shapes)
        and (request.paused is None or request.paused == row.paused)
    )


def _schedule_filter_is_unknown(row: ScheduleSummaryFact, request: ListSchedulesRequest) -> bool:
    return (request.paused is not None and row.paused is None) or (
        request.shape is not None and not row.shapes
    )


def _schedule_summary(row: ScheduleSummaryFact) -> ScheduleSummaryOutput:
    return ScheduleSummaryOutput(
        schedule_id=row.schedule_id,
        paused=row.paused,
        note=row.note,
        shapes=row.shapes,
        recent_action_times=row.recent_action_times,
        next_action_times=row.next_action_times,
    )


def _schedule_description(row: ScheduleDescriptionFact) -> DescribeScheduleAnswer:
    return DescribeScheduleAnswer(
        schedule_id=row.schedule_id,
        action=ScheduleActionOutput(**row.action.__dict__),
        paused=row.paused,
        note=row.note,
        limited_actions=row.limited_actions,
        remaining_actions=row.remaining_actions,
        shapes=row.shapes,
        cron_expressions=row.cron_expressions,
        intervals_seconds=row.intervals_seconds,
        calendar_count=row.calendar_count,
        time_zone_name=row.time_zone_name,
        overlap_policy=row.overlap_policy,
        catchup_window_seconds=row.catchup_window_seconds,
        pause_on_failure=row.pause_on_failure,
        recent_action_times=row.recent_action_times,
        next_action_times=row.next_action_times,
        upcoming_action_times=row.upcoming_action_times,
        observed_at=row.observed_at,
    )


def _batch_matches(row: BatchOperationSummaryFact, request: ListBatchOperationsRequest) -> bool:
    return (request.state is None or row.state == request.state) and (
        request.job_type is None or row.operation_type == request.job_type
    )


def _batch_output(
    summary: BatchOperationSummaryFact,
    detail: BatchOperationDetailFact | None,
    status: str,
) -> BatchOperationOutput:
    return BatchOperationOutput(
        job_id=summary.job_id,
        state=detail.state if detail else summary.state,
        operation_type=detail.operation_type if detail else summary.operation_type,
        start_time=detail.start_time if detail else summary.start_time,
        close_time=detail.close_time if detail else summary.close_time,
        detail_status=status,
        total_operation_count=detail.total_operation_count if detail else None,
        complete_operation_count=detail.complete_operation_count if detail else None,
        failure_operation_count=detail.failure_operation_count if detail else None,
        identity=detail.identity if detail else None,
        reason=detail.reason if detail else None,
        query=detail.query if detail else None,
    )


def _inspection(
    context: RequestContext,
    before: BudgetSnapshot,
    completion: Completeness,
    *,
    limit: int | None = None,
) -> Inspection:
    after = context.budget.snapshot()
    return Inspection(
        completion=completion,
        temporal_calls=after.temporal_calls - before.temporal_calls,
        pages_read=after.pages_read - before.pages_read,
        limit=limit,
    )


ResponseModel = TypeVar("ResponseModel", bound=ContractModel)


def _charge_output(context: RequestContext, response: ResponseModel) -> ResponseModel:
    context.budget.consume_output_bytes(len(response.model_dump_json().encode("utf-8")))
    return response


def _encode_token(value: bytes | None) -> str | None:
    return base64.urlsafe_b64encode(value).decode("ascii") if value else None


def _decode_token(value: str | None) -> bytes | None:
    if value is None:
        return None
    try:
        return base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, ValueError, TypeError, binascii.Error) as exc:
        raise OpsMcpError(
            ErrorCode.INVALID_ARGUMENT,
            "The page token is invalid",
            retryable=False,
        ) from exc
