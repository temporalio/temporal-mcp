"""Public schema-v1 contracts for configured target resolution and discovery."""

from typing import Annotated, Literal

from pydantic import Field

from ops_mcp.contracts.primitives import (
    CURRENT_SCHEMA_VERSION,
    ContractModel,
    Inspection,
    SchemaVersion,
    WarningMessage,
)

NonEmptyString = Annotated[str, Field(min_length=1)]


class ResolveTargetRequest(ContractModel):
    human_name: NonEmptyString | None = None


class TargetCandidate(ContractModel):
    target_ref: NonEmptyString
    display_name: NonEmptyString
    account: NonEmptyString
    cluster: NonEmptyString
    namespace: NonEmptyString


class ResolvedTargetAnswer(ContractModel):
    kind: Literal["resolved"] = "resolved"
    target_ref: NonEmptyString
    display_name: NonEmptyString
    account: NonEmptyString
    cluster: NonEmptyString
    namespace: NonEmptyString
    resolved_from: NonEmptyString
    resolution: Literal["exact_name_or_alias", "sole_authorized_target"] = "exact_name_or_alias"


class TargetCandidatesAnswer(ContractModel):
    kind: Literal["candidates"] = "candidates"
    reason: Literal["discovery", "ambiguous"]
    candidates: tuple[TargetCandidate, ...]
    requested_name: NonEmptyString | None = None


TargetResolutionAnswer = Annotated[
    ResolvedTargetAnswer | TargetCandidatesAnswer,
    Field(discriminator="kind"),
]


class ResolveTargetResponse(ContractModel):
    schema_version: SchemaVersion = CURRENT_SCHEMA_VERSION
    answer: TargetResolutionAnswer
    inspection: Inspection
    warnings: tuple[WarningMessage, ...] = ()

    @property
    def target_ref(self) -> str | None:
        """In-process compatibility projection for exact-resolution consumers."""
        if isinstance(self.answer, ResolvedTargetAnswer):
            return self.answer.target_ref
        return None
