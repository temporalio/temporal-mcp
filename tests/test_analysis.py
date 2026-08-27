from temporalio.api.enums.v1 import EventType, RetryState
from temporalio.api.history.v1 import HistoryEvent

from ops_mcp.analysis import build_reset_plan, extract_failures


def event(event_id: int, event_type: EventType) -> HistoryEvent:
    return HistoryEvent(event_id=event_id, event_type=event_type)


def failed_order_history() -> list[HistoryEvent]:
    events = [
        event(1, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED),
        event(2, EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
        event(3, EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED),
        event(4, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED),
        event(5, EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED),
        event(6, EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED),
        event(7, EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED),
        event(8, EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
        event(9, EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED),
        event(10, EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED),
        event(11, EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED),
        event(12, EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED),
        event(13, EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED),
        event(14, EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED),
        event(15, EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED),
        event(16, EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED),
    ]
    payment = events[4].activity_task_scheduled_event_attributes
    payment.activity_id = "payment"
    payment.activity_type.name = "AuthorizePayment"
    events[5].activity_task_started_event_attributes.scheduled_event_id = 5
    events[5].activity_task_started_event_attributes.attempt = 1
    events[6].activity_task_completed_event_attributes.scheduled_event_id = 5

    enrich = events[10].activity_task_scheduled_event_attributes
    enrich.activity_id = "enrich"
    enrich.activity_type.name = "EnrichProduct"
    started = events[11].activity_task_started_event_attributes
    started.scheduled_event_id = 11
    started.attempt = 3
    failed = events[12].activity_task_failed_event_attributes
    failed.scheduled_event_id = 11
    failed.started_event_id = 12
    failed.retry_state = RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
    failed.failure.message = "activity failed"
    failed.failure.cause.message = "503 Service Unavailable"
    failed.failure.cause.application_failure_info.type = "CatalogServiceUnavailable"
    events[15].workflow_execution_failed_event_attributes.failure.CopyFrom(failed.failure)
    return events


def test_extracts_activity_attempt_and_root_cause() -> None:
    failures = extract_failures(failed_order_history())
    activity = failures[0]
    assert activity.activity_type == "EnrichProduct"
    assert activity.attempt == 3
    assert activity.retry_state == "MAXIMUM_ATTEMPTS_REACHED"
    assert "503 Service Unavailable" in activity.message


def test_reset_plan_recommends_before_failed_activity() -> None:
    plan = build_reset_plan("order-88231", "run-1", failed_order_history(), None)
    assert plan["recommended_event_id"] == 10
    assert plan["already_completed_before_reset"] == ["AuthorizePayment"]
    assert plan["activities_that_would_replay"] == ["EnrichProduct"]
    assert "--workflow-id order-88231" in plan["command"]
    assert "--run-id run-1" in plan["command"]
    assert plan["read_only"] is True
