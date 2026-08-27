from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ops_mcp.contracts import (
    ResolvedTargetAnswer,
    ResolveTargetRequest,
    TargetCandidatesAnswer,
)
from ops_mcp.runtime import (
    BasicBudgetLimits,
    BasicExecutionBudget,
    Principal,
    RequestContext,
    ResultTooLargeError,
    TargetNotFoundError,
)
from ops_mcp.services import DefaultTargetService
from ops_mcp.targets import YamlTargetCatalog


def _context(*, groups: tuple[str, ...], output_bytes: int = 1_000_000) -> RequestContext:
    return RequestContext(
        request_id="target-discovery",
        principal=Principal(subject="operator", groups=groups),
        budget=BasicExecutionBudget(
            deadline=datetime.now(UTC) + timedelta(seconds=30),
            limits=BasicBudgetLimits(output_bytes=output_bytes),
        ),
    )


def _catalog(tmp_path: Path) -> YamlTargetCatalog:
    config = tmp_path / "targets.yaml"
    config.write_text(
        """
groups:
  support:
    targets:
      orders-prod:
        aliases: [orders prod]
        account: acme
        cluster: prod
        namespace: orders
        address: orders.example:7233
        tls: true
      payments-prod:
        aliases: [production, payments prod]
        account: acme
        cluster: prod
        namespace: payments
        address: payments.example:7233
        tls: true
  platform:
    targets:
      orders-prod:
        aliases: [production orders, orders prod]
        account: acme
        cluster: prod
        namespace: orders
        address: orders.example:7233
        tls: true
      fulfillment-prod:
        aliases: [production, fulfillment prod]
        account: acme
        cluster: prod
        namespace: fulfillment
        address: fulfillment.example:7233
        tls: true
  restricted:
    targets:
      secret-prod:
        aliases: [secret production]
        account: hidden
        cluster: hidden
        namespace: hidden
        address: secret.example:7233
        tls: true
""",
        encoding="utf-8",
    )
    return YamlTargetCatalog.load(config)


def test_no_name_discovers_authorized_targets(tmp_path: Path) -> None:
    result = DefaultTargetService(_catalog(tmp_path)).resolve_target(
        _context(groups=("support",)), ResolveTargetRequest()
    )

    assert isinstance(result.answer, TargetCandidatesAnswer)
    assert result.answer.reason == "discovery"
    assert [candidate.target_ref for candidate in result.answer.candidates] == [
        "orders-prod",
        "payments-prod",
    ]
    assert result.inspection.temporal_calls == 0


def test_no_name_can_discover_one_authorized_target(tmp_path: Path) -> None:
    result = DefaultTargetService(_catalog(tmp_path)).resolve_target(
        _context(groups=("restricted",)), ResolveTargetRequest()
    )

    assert isinstance(result.answer, ResolvedTargetAnswer)
    assert result.answer.target_ref == "secret-prod"
    assert result.answer.resolution == "sole_authorized_target"


def test_discovery_is_deterministic_and_deduplicates_across_groups(tmp_path: Path) -> None:
    service = DefaultTargetService(_catalog(tmp_path))

    first = service.resolve_target(_context(groups=("platform", "support")), ResolveTargetRequest())
    second = service.resolve_target(
        _context(groups=("support", "platform")), ResolveTargetRequest()
    )

    assert isinstance(first.answer, TargetCandidatesAnswer)
    assert isinstance(second.answer, TargetCandidatesAnswer)
    assert first.answer.candidates == second.answer.candidates
    assert [candidate.target_ref for candidate in first.answer.candidates] == [
        "fulfillment-prod",
        "orders-prod",
        "payments-prod",
    ]
    assert sum(c.target_ref == "orders-prod" for c in first.answer.candidates) == 1


def test_normalized_exact_match_preserves_resolved_shape(tmp_path: Path) -> None:
    result = DefaultTargetService(_catalog(tmp_path)).resolve_target(
        _context(groups=("support",)),
        ResolveTargetRequest(human_name="Orders_Prod"),
    )

    assert isinstance(result.answer, ResolvedTargetAnswer)
    assert result.answer.kind == "resolved"
    assert result.answer.target_ref == "orders-prod"
    assert result.target_ref == "orders-prod"
    assert "address" not in result.model_dump_json()


def test_exact_alias_ambiguity_returns_public_candidates(tmp_path: Path) -> None:
    result = DefaultTargetService(_catalog(tmp_path)).resolve_target(
        _context(groups=("support", "platform")),
        ResolveTargetRequest(human_name="production"),
    )

    assert isinstance(result.answer, TargetCandidatesAnswer)
    assert result.answer.reason == "ambiguous"
    assert result.answer.requested_name == "production"
    assert [candidate.target_ref for candidate in result.answer.candidates] == [
        "fulfillment-prod",
        "payments-prod",
    ]
    serialized = result.model_dump_json()
    assert "address" not in serialized
    assert "secret" not in serialized


@pytest.mark.parametrize("name", ["secret production", "does not exist"])
def test_lookup_miss_does_not_disclose_unauthorized_existence(tmp_path: Path, name: str) -> None:
    with pytest.raises(TargetNotFoundError) as exc_info:
        DefaultTargetService(_catalog(tmp_path)).resolve_target(
            _context(groups=("support",)), ResolveTargetRequest(human_name=name)
        )

    assert exc_info.value.code.value == "target_not_found"
    assert exc_info.value.details is None


def test_discovery_charges_output_budget(tmp_path: Path) -> None:
    with pytest.raises(ResultTooLargeError):
        DefaultTargetService(_catalog(tmp_path)).resolve_target(
            _context(groups=("support", "platform"), output_bytes=1),
            ResolveTargetRequest(),
        )
