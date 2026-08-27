from ops_mcp.demo_stuck.seed import (
    EXTRA_ACTIVITY_WORKFLOWS,
    MAIN_WORKFLOW_ID,
    POPULATIONS,
    TOTAL_ACTIVITIES,
    TOTAL_WORKFLOWS,
)


def test_session_two_fixture_totals_and_types_are_stable() -> None:
    assert MAIN_WORKFLOW_ID == "session2-payment-reconciliation-main"
    assert sum(population.count for population in POPULATIONS) == TOTAL_WORKFLOWS == 1_830
    assert len({population.workflow.__name__ for population in POPULATIONS}) == 4
    assert (TOTAL_WORKFLOWS * 2) + EXTRA_ACTIVITY_WORKFLOWS == TOTAL_ACTIVITIES == 4_200
