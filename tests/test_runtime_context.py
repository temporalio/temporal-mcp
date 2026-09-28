from datetime import timedelta

from ops_mcp.runtime import (
    BasicBudgetLimits,
    DemoIdentityProvider,
    Principal,
    RequestContextFactory,
)


class StaticIdentity:
    def current_principal(self) -> Principal:
        return Principal(subject="operator-1", groups=("support",))


def test_request_context_factory_creates_request_scoped_budget() -> None:
    factory = RequestContextFactory(
        StaticIdentity(),
        default_timeout=timedelta(seconds=5),
        default_limits=BasicBudgetLimits(temporal_calls=3),
    )

    first = factory.create(request_id="request-1")
    second = factory.create(request_id="request-2")
    first.budget.reserve_temporal_call("one")

    assert first.principal.subject == "operator-1"
    assert first.request_id == "request-1"
    assert first.budget.snapshot().temporal_calls == 1
    assert second.budget.snapshot().temporal_calls == 0


def test_request_context_can_apply_tool_specific_basic_limits() -> None:
    factory = RequestContextFactory(StaticIdentity())

    context = factory.create(limits=BasicBudgetLimits(temporal_calls=101))

    assert context.budget.limits.temporal_calls == 101


def test_environment_identity_prefers_operator_group_variable(monkeypatch) -> None:
    monkeypatch.setenv("OPS_MCP_GROUP", "operators")
    monkeypatch.setenv("OPS_MCP_DEMO_GROUP", "legacy-demo")

    principal = DemoIdentityProvider().current_principal()

    assert principal.groups == ("operators",)


def test_environment_identity_accepts_legacy_group_variable(monkeypatch) -> None:
    monkeypatch.delenv("OPS_MCP_GROUP", raising=False)
    monkeypatch.setenv("OPS_MCP_DEMO_GROUP", "legacy-demo")

    principal = DemoIdentityProvider().current_principal()

    assert principal.groups == ("legacy-demo",)


def test_environment_identity_defaults_to_operators(monkeypatch) -> None:
    monkeypatch.delenv("OPS_MCP_GROUP", raising=False)
    monkeypatch.delenv("OPS_MCP_DEMO_GROUP", raising=False)

    principal = DemoIdentityProvider().current_principal()

    assert principal.groups == ("operators",)
