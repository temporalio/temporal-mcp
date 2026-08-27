"""Public schema-v1 contracts for Temporal workflow visibility reads."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, SerializerFunctionWrapHandler, field_validator, model_serializer

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    ContractModel,
    Inspection,
    SchemaVersion,
    WarningMessage,
)

NonEmptyString = Annotated[str, Field(min_length=1)]


class WorkflowField(StrEnum):
    WORKFLOW_ID = "workflow_id"
    RUN_ID = "run_id"
    WORKFLOW_TYPE = "workflow_type"
    STATUS = "status"
    TASK_QUEUE = "task_queue"
    START_TIME = "start_time"
    EXECUTION_TIME = "execution_time"
    CLOSE_TIME = "close_time"
    HISTORY_LENGTH = "history_length"
    PARENT_WORKFLOW_ID = "parent_workflow_id"
    PARENT_RUN_ID = "parent_run_id"
    ROOT_WORKFLOW_ID = "root_workflow_id"
    ROOT_RUN_ID = "root_run_id"

    @classmethod
    def _missing_(cls, value: object) -> WorkflowField | None:
        if not isinstance(value, str):
            return None
        normalized = value.replace("-", "_").casefold()
        aliases = {
            "workflowid": cls.WORKFLOW_ID,
            "runid": cls.RUN_ID,
            "workflowtype": cls.WORKFLOW_TYPE,
            "executionstatus": cls.STATUS,
            "taskqueue": cls.TASK_QUEUE,
            "starttime": cls.START_TIME,
            "executiontime": cls.EXECUTION_TIME,
            "closetime": cls.CLOSE_TIME,
            "historylength": cls.HISTORY_LENGTH,
            "parentworkflowid": cls.PARENT_WORKFLOW_ID,
            "parentrunid": cls.PARENT_RUN_ID,
            "rootworkflowid": cls.ROOT_WORKFLOW_ID,
            "rootrunid": cls.ROOT_RUN_ID,
        }
        return aliases.get(normalized.replace("_", ""))


class FilterWorkflowsRequest(ContractModel):
    target_ref: NonEmptyString
    query: str
    fields: tuple[WorkflowField, ...] = Field(
        default=(
            WorkflowField.WORKFLOW_ID,
            WorkflowField.RUN_ID,
            WorkflowField.WORKFLOW_TYPE,
            WorkflowField.STATUS,
            WorkflowField.START_TIME,
        ),
        min_length=1,
    )
    custom_search_attributes: tuple[NonEmptyString, ...] = Field(default=(), max_length=10)
    page_size: int = Field(default=100, ge=1, le=200)
    page_token: str | None = Field(default=None, min_length=1)

    @field_validator("fields")
    @classmethod
    def fields_must_be_unique(cls, fields: tuple[WorkflowField, ...]) -> tuple[WorkflowField, ...]:
        if len(fields) != len(set(fields)):
            raise ValueError("fields must not contain duplicates")
        return fields

    @field_validator("custom_search_attributes")
    @classmethod
    def custom_search_attributes_must_be_unique(cls, names: tuple[str, ...]) -> tuple[str, ...]:
        if len(names) != len(set(names)):
            raise ValueError("custom_search_attributes must not contain duplicates")
        return names


SearchAttributeValue = str | int | float | bool | datetime | tuple[str, ...] | None


class SearchAttributeProjection(ContractModel):
    name: NonEmptyString
    value_type: PublicIndexedValueType
    status: Literal["present", "not_set", "truncated", "unsupported"]
    value: SearchAttributeValue = None
    value_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class WorkflowProjectionRow(ContractModel):
    """Typed row whose serialization contains exactly the requested fields."""

    workflow_id: str | None = None
    run_id: str | None = None
    workflow_type: str | None = None
    status: str | None = None
    task_queue: str | None = None
    start_time: datetime | None = None
    execution_time: datetime | None = None
    close_time: datetime | None = None
    history_length: int | None = Field(default=None, ge=0)
    parent_workflow_id: str | None = None
    parent_run_id: str | None = None
    root_workflow_id: str | None = None
    root_run_id: str | None = None
    search_attributes: tuple[SearchAttributeProjection, ...] = ()

    @model_serializer(mode="wrap")
    def serialize_projection(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        # `model_fields_set` distinguishes an unrequested field from a requested null value.
        serialized = handler(self)
        return {name: serialized[name] for name in self.model_fields_set}


class FilterWorkflowsAnswer(ContractModel):
    query: str
    custom_search_attributes: tuple[NonEmptyString, ...] = ()
    workflows: tuple[WorkflowProjectionRow, ...]
    returned: int = Field(ge=0)
    next_page_token: str | None = None


class FilterWorkflowsResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: FilterWorkflowsAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class SearchWorkflowRunsAnswer(ContractModel):
    query: str
    custom_search_attributes: tuple[NonEmptyString, ...] = ()
    workflows: tuple[WorkflowProjectionRow, ...]
    returned: int = Field(ge=0)
    continuation_token: str | None = None


class SearchWorkflowRunsResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: SearchWorkflowRunsAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class WorkflowCountGroupBy(StrEnum):
    """Server-supported grouping on the pinned Temporal backend."""

    STATUS = "status"


class CountWorkflowsRequest(ContractModel):
    target_ref: NonEmptyString
    query: str
    group_by: WorkflowCountGroupBy | None = None


class WorkflowCountGroup(ContractModel):
    key: NonEmptyString
    count: int = Field(ge=0)


class CountWorkflowsAnswer(ContractModel):
    query: str
    group_by: WorkflowCountGroupBy | None
    count: int = Field(ge=0)
    groups: tuple[WorkflowCountGroup, ...]
    precision: Literal["approximate"] = "approximate"


class CountWorkflowsResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: CountWorkflowsAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()


class GetVisibilitySchemaRequest(ContractModel):
    target_ref: NonEmptyString


class PublicSearchAttributeSource(StrEnum):
    SYSTEM = "system"
    CUSTOM = "custom"


class PublicIndexedValueType(StrEnum):
    TEXT = "text"
    KEYWORD = "keyword"
    INT = "int"
    DOUBLE = "double"
    BOOL = "bool"
    DATETIME = "datetime"
    KEYWORD_LIST = "keyword_list"
    UNKNOWN = "unknown"


class VisibilityField(ContractModel):
    name: NonEmptyString
    source: PublicSearchAttributeSource
    value_type: PublicIndexedValueType
    query_examples: tuple[NonEmptyString, ...] = ()


class VisibilitySchemaAnswer(ContractModel):
    namespace: NonEmptyString
    fields: tuple[VisibilityField, ...]


class GetVisibilitySchemaResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    target_ref: NonEmptyString
    answer: VisibilitySchemaAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()
