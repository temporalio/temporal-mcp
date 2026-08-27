from ops_mcp.demo_incident.ground_truth import (
    EXPECTED_DURATION_SECONDS,
    PAYMENT_RUNS,
    POPULATIONS,
    TEAMS,
    TOTAL_RUNS,
)
from ops_mcp.demo_incident.ground_truth import (
    operator_prompt as incident_prompt,
)
from ops_mcp.demo_incident.workflows import WORKFLOWS
from ops_mcp.demo_nondeterminism.ground_truth import (
    AFFECTED_ORDINALS,
    AFFECTED_WORKFLOW_IDS,
    EXPOSED_RUNS,
    NONDETERMINISTIC_RUNS,
    TOTAL_CANDIDATES,
)
from ops_mcp.demo_nondeterminism.ground_truth import (
    operator_prompt as nondeterminism_prompt,
)
from ops_mcp.demo_nondeterminism.workflows_v1 import FulfillmentWorkflowV1
from ops_mcp.demo_nondeterminism.workflows_v2 import FulfillmentWorkflowV2


def test_session_three_preserves_candidate_exposure_and_failure_boundaries() -> None:
    assert TOTAL_CANDIDATES == 230
    assert EXPOSED_RUNS == 190
    assert NONDETERMINISTIC_RUNS == 43
    assert len(AFFECTED_ORDINALS) == NONDETERMINISTIC_RUNS
    assert len(AFFECTED_WORKFLOW_IDS) == NONDETERMINISTIC_RUNS
    assert max(AFFECTED_ORDINALS) < EXPOSED_RUNS
    assert FulfillmentWorkflowV1.__temporal_workflow_definition.name == "FulfillmentWorkflow"
    assert FulfillmentWorkflowV2.__temporal_workflow_definition.name == "FulfillmentWorkflow"
    prompt = nondeterminism_prompt("2026-08-24T21:20:00Z")
    assert "fulfillment-v2" in prompt
    assert "How many, and which ones?" in prompt


def test_session_four_preserves_team_domain_and_customer_question() -> None:
    assert sum(item.count for item in POPULATIONS) == TOTAL_RUNS == 160
    assert sum(item.count for item in POPULATIONS if item.domain == "payments") == 90
    assert PAYMENT_RUNS == 90
    assert len(TEAMS) == 6
    assert len(WORKFLOWS) == len(POPULATIONS) == 6
    assert EXPECTED_DURATION_SECONDS == 5
    prompt = incident_prompt("2026-08-24T10:00:00Z", "2026-08-24T10:40:00Z")
    assert "Which teams' workflows were affected" in prompt
    assert "which ones touch money" in prompt
    assert "configured normal duration" in prompt
