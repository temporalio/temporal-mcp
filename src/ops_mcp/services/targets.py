"""Configured-target resolution service."""

from __future__ import annotations

from typing import Protocol

from ops_mcp.contracts.primitives import Completeness, Inspection
from ops_mcp.contracts.targets import (
    ResolvedTargetAnswer,
    ResolveTargetRequest,
    ResolveTargetResponse,
    TargetCandidate,
    TargetCandidatesAnswer,
)
from ops_mcp.runtime import RequestContext
from ops_mcp.runtime.errors import TargetNotFoundError
from ops_mcp.targets import TargetCatalog
from ops_mcp.targets.models import ResolvedTarget


class TargetService(Protocol):
    def resolve_target(
        self,
        context: RequestContext,
        request: ResolveTargetRequest,
    ) -> ResolveTargetResponse: ...


class DefaultTargetService:
    def __init__(self, target_catalog: TargetCatalog) -> None:
        self._target_catalog = target_catalog

    def resolve_target(
        self,
        context: RequestContext,
        request: ResolveTargetRequest,
    ) -> ResolveTargetResponse:
        context.budget.ensure_active()
        if request.human_name is None:
            targets = self._target_catalog.list_authorized(context.principal)
            if len(targets) == 1:
                target = targets[0]
                answer = ResolvedTargetAnswer(
                    target_ref=target.target_ref,
                    display_name=_display_name(target),
                    account=target.account,
                    cluster=target.cluster,
                    namespace=target.namespace,
                    resolved_from=_display_name(target),
                    resolution="sole_authorized_target",
                )
            else:
                answer = TargetCandidatesAnswer(
                    reason="discovery",
                    candidates=tuple(_public_candidate(target) for target in targets),
                )
        else:
            targets = self._target_catalog.match_authorized(context.principal, request.human_name)
            if not targets:
                # Authorized-only matching deliberately makes unknown and unauthorized
                # names indistinguishable at this boundary.
                raise TargetNotFoundError(request.human_name)
            if len(targets) > 1:
                answer = TargetCandidatesAnswer(
                    reason="ambiguous",
                    requested_name=request.human_name,
                    candidates=tuple(_public_candidate(target) for target in targets),
                )
            else:
                target = targets[0]
                answer = ResolvedTargetAnswer(
                    target_ref=target.target_ref,
                    display_name=_display_name(target),
                    account=target.account,
                    cluster=target.cluster,
                    namespace=target.namespace,
                    resolved_from=request.human_name,
                )
        response = ResolveTargetResponse(
            answer=answer,
            inspection=Inspection(
                completion=Completeness.COMPLETE,
                temporal_calls=0,
                pages_read=0,
            ),
        )
        context.budget.consume_output_bytes(len(response.model_dump_json().encode("utf-8")))
        return response


def _display_name(target: ResolvedTarget) -> str:
    return target.aliases[0] if target.aliases else target.target_ref


def _public_candidate(target: ResolvedTarget) -> TargetCandidate:
    return TargetCandidate(
        target_ref=target.target_ref,
        display_name=_display_name(target),
        account=target.account,
        cluster=target.cluster,
        namespace=target.namespace,
    )
