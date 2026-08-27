import pytest
from pydantic import ValidationError

from ops_mcp.contracts import (
    CURRENT_SCHEMA_VERSION,
    Completeness,
    Confidence,
    EvidenceRef,
    EvidenceSource,
    Finding,
    Inspection,
)
from ops_mcp.contracts.visibility import FilterWorkflowsRequest, WorkflowField


def test_shared_contract_values_are_strict_and_versioned() -> None:
    inspection = Inspection(
        completion=Completeness.COMPLETE,
        temporal_calls=2,
        pages_read=1,
        events_read=10,
    )
    finding = Finding(
        code="activity_retry_exhausted",
        summary="The activity exhausted its configured attempts",
        confidence=Confidence.HIGH,
        evidence=(EvidenceRef(source=EvidenceSource.HISTORY_EVENT, reference="event:13"),),
    )

    assert CURRENT_SCHEMA_VERSION == "1"
    assert inspection.completion == "complete"
    assert finding.evidence[0].reference == "event:13"

    with pytest.raises(ValidationError):
        Inspection(completion=Completeness.COMPLETE, temporal_calls=-1, pages_read=0)


def test_search_projection_defaults_and_temporal_field_aliases() -> None:
    request = FilterWorkflowsRequest(target_ref="orders-prod", query="")

    assert request.fields[:2] == (WorkflowField.WORKFLOW_ID, WorkflowField.RUN_ID)
    assert WorkflowField("WorkflowId") is WorkflowField.WORKFLOW_ID
    assert WorkflowField("ExecutionStatus") is WorkflowField.STATUS
