from datetime import timedelta

from ops_mcp.runtime import (
    BasicBudgetLimits,
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
