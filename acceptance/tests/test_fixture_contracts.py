from ops_mcp.demo_incident.ground_truth import (
    EXPECTED_DURATION_SECONDS,
    PAYMENT_RUNS,
    POPULATIONS,
    TEAMS,
    TOTAL_RUNS,
)
from ops_mcp.demo_nondeterminism.ground_truth import (
    AFFECTED_ORDINALS,
    AFFECTED_WORKFLOW_IDS,
    EXPOSED_RUNS,
    NONDETERMINISTIC_RUNS,
    TOTAL_CANDIDATES,
)
from ops_mcp.demo_stuck.seed import (
    EXTRA_ACTIVITY_WORKFLOWS,
    MAIN_WORKFLOW_ID,
    TOTAL_ACTIVITIES,
    TOTAL_WORKFLOWS,
)
from ops_mcp.demo_stuck.seed import (
    POPULATIONS as STUCK_POPULATIONS,
)


def test_session_two_population_is_the_historical_acceptance_fixture() -> None:
    assert MAIN_WORKFLOW_ID == "session2-payment-reconciliation-main"
    assert sum(population.count for population in STUCK_POPULATIONS) == TOTAL_WORKFLOWS == 1_830
    assert len({population.workflow.__name__ for population in STUCK_POPULATIONS}) == 4
    assert (TOTAL_WORKFLOWS * 2) + EXTRA_ACTIVITY_WORKFLOWS == TOTAL_ACTIVITIES == 4_200


def test_session_three_boundaries_are_stable() -> None:
    assert TOTAL_CANDIDATES == 230
    assert EXPOSED_RUNS == 190
    assert NONDETERMINISTIC_RUNS == 43
    assert len(AFFECTED_ORDINALS) == NONDETERMINISTIC_RUNS
    assert len(AFFECTED_WORKFLOW_IDS) == NONDETERMINISTIC_RUNS
    assert max(AFFECTED_ORDINALS) < EXPOSED_RUNS


def test_session_four_population_is_stable() -> None:
    assert sum(item.count for item in POPULATIONS) == TOTAL_RUNS == 160
    assert sum(item.count for item in POPULATIONS if item.domain == "payments") == 90
    assert PAYMENT_RUNS == 90
    assert len(TEAMS) == 6
    assert EXPECTED_DURATION_SECONDS == 5
