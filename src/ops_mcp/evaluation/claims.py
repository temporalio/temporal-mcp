"""Evidence-to-claim grading for the honest Session 3 and Session 4 demos.

The grader intentionally consumes structured claims rather than matching answer prose.
An evaluation adapter (human-authored or model-assisted) records what the answer claims,
the claim's certainty and scope, and the tool evidence it cites. These deterministic rules
then reject the known high-severity attribution errors.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, model_validator

from ops_mcp.contracts.primitives import ContractModel


class EvaluationScenario(StrEnum):
    NONDETERMINISM = "session_3_nondeterminism"
    INCIDENT_IMPACT = "session_4_incident_impact"


class EvidenceKind(StrEnum):
    VISIBILITY_CANDIDATES = "visibility_candidates"
    TEMPORAL_REPORTED_PROBLEM = "temporal_reported_problem"
    BUILD_ID_SUCCESS = "build_id_success"
    HISTORY_INSPECTION = "history_inspection"
    SEARCH_ATTRIBUTE_GROUP = "search_attribute_group"
    CURRENT_TASK_QUEUE = "current_task_queue"
    EXTERNAL_POLICY = "external_policy"


class ClaimKind(StrEnum):
    CANDIDATE_COUNT = "candidate_count"
    REPORTED_PROBLEM_COUNT = "reported_problem_count"
    BUILD_SUCCESS_COUNT = "build_success_count"
    EXPOSED_COUNT = "exposed_count"
    CONFIRMED_FAILURE_COUNT = "confirmed_failure_count"
    ESTIMATED_FAILURE_COUNT = "estimated_failure_count"
    SOURCE_DETAIL = "source_detail"
    CONFIRMED_INCIDENT_COUNT = "confirmed_incident_count"
    TEAM_BREAKDOWN = "team_breakdown"
    MONEY_BREAKDOWN = "money_breakdown"
    HISTORICAL_QUEUE_STATE = "historical_queue_state"
    CURRENT_QUEUE_STATE = "current_queue_state"


class ClaimCertainty(StrEnum):
    EXACT = "exact"
    ESTIMATE = "estimate"
    LOWER_BOUND = "lower_bound"
    QUALIFIED = "qualified"


class ClaimScope(StrEnum):
    CANDIDATE_POPULATION = "candidate_population"
    REPORTED_PROBLEM_POPULATION = "reported_problem_population"
    BUILD_SUCCESS_POPULATION = "build_success_population"
    INSPECTED_SAMPLE = "inspected_sample"
    CONFIRMED_POPULATION = "confirmed_population"
    CURRENT_SNAPSHOT = "current_snapshot"
    HISTORICAL_WINDOW = "historical_window"


class EvidenceRecord(ContractModel):
    """One normalized fact from a tool result used by an answer claim."""

    evidence_id: str = Field(min_length=1)
    kind: EvidenceKind
    description: str = Field(min_length=1)
    scope: ClaimScope
    exact: bool = False
    complete: bool = False
    count: int | None = Field(default=None, ge=0)
    population_count: int | None = Field(default=None, ge=0)
    inspected_count: int | None = Field(default=None, ge=0)
    matched_count: int | None = Field(default=None, ge=0)
    cause: str | None = None
    search_attribute: str | None = None
    groups: dict[str, int] = Field(default_factory=dict)
    workflow_ids: tuple[str, ...] = ()
    source_details: tuple[str, ...] = ()
    observed_at: str | None = None

    @model_validator(mode="after")
    def validate_kind_fields(self) -> EvidenceRecord:
        if self.kind is EvidenceKind.HISTORY_INSPECTION:
            if self.inspected_count is None or self.matched_count is None:
                raise ValueError("history inspection requires inspected_count and matched_count")
        if self.kind is EvidenceKind.TEMPORAL_REPORTED_PROBLEM and not self.cause:
            raise ValueError("reported-problem evidence requires cause")
        if self.kind is EvidenceKind.SEARCH_ATTRIBUTE_GROUP:
            if not self.search_attribute or not self.groups:
                raise ValueError("search-attribute grouping requires a field and groups")
        if self.kind is EvidenceKind.CURRENT_TASK_QUEUE and not self.observed_at:
            raise ValueError("current task-queue evidence requires observed_at")
        return self


class AnswerClaim(ContractModel):
    """A material claim extracted from an answer without depending on its wording."""

    claim_id: str = Field(min_length=1)
    kind: ClaimKind
    statement: str = Field(min_length=1)
    certainty: ClaimCertainty
    scope: ClaimScope
    evidence_ids: tuple[str, ...]
    count: int | None = Field(default=None, ge=0)
    groups: dict[str, int] = Field(default_factory=dict)
    search_attribute: str | None = None
    source_detail: str | None = None


class AnswerManifest(ContractModel):
    """JSON-serializable evidence and claim record for one agent answer."""

    scenario: EvaluationScenario
    evidence: tuple[EvidenceRecord, ...]
    claims: tuple[AnswerClaim, ...]

    @model_validator(mode="after")
    def require_unique_ids(self) -> AnswerManifest:
        evidence_ids = [item.evidence_id for item in self.evidence]
        claim_ids = [item.claim_id for item in self.claims]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("evidence_id values must be unique")
        if len(claim_ids) != len(set(claim_ids)):
            raise ValueError("claim_id values must be unique")
        return self


class EvaluationViolation(ContractModel):
    code: str
    claim_id: str
    message: str


class AnswerEvaluation(ContractModel):
    passed: bool
    violations: tuple[EvaluationViolation, ...]


def grade_answer(manifest: AnswerManifest) -> AnswerEvaluation:
    """Apply deterministic high-severity attribution rules to an answer manifest."""

    evidence = {item.evidence_id: item for item in manifest.evidence}
    violations: list[EvaluationViolation] = []
    for claim in manifest.claims:
        cited = tuple(evidence[item] for item in claim.evidence_ids if item in evidence)
        missing = tuple(item for item in claim.evidence_ids if item not in evidence)
        if not claim.evidence_ids or missing:
            detail = "no evidence was cited" if not claim.evidence_ids else f"missing: {missing!r}"
            violations.append(_violation("missing_evidence", claim, detail))
            continue
        violations.extend(_grade_common_support(claim, cited))
        if manifest.scenario is EvaluationScenario.NONDETERMINISM:
            violations.extend(_grade_session_three(claim, cited))
        elif manifest.scenario is EvaluationScenario.INCIDENT_IMPACT:
            violations.extend(_grade_session_four(claim, cited))
    return AnswerEvaluation(passed=not violations, violations=tuple(violations))


def _grade_common_support(
    claim: AnswerClaim, cited: tuple[EvidenceRecord, ...]
) -> tuple[EvaluationViolation, ...]:
    expected_count_kind = {
        ClaimKind.CANDIDATE_COUNT: EvidenceKind.VISIBILITY_CANDIDATES,
        ClaimKind.BUILD_SUCCESS_COUNT: EvidenceKind.BUILD_ID_SUCCESS,
    }.get(claim.kind)
    if expected_count_kind is not None:
        candidates = tuple(item for item in cited if item.kind is expected_count_kind)
        exact_required = claim.certainty is ClaimCertainty.EXACT
        supported = claim.count is not None and any(
            item.count == claim.count and (item.exact or not exact_required) for item in candidates
        )
        if not supported:
            return (
                _violation(
                    "claim_value_not_supported",
                    claim,
                    f"the cited {expected_count_kind.value} evidence does not support this value",
                ),
            )
    if claim.kind is ClaimKind.CURRENT_QUEUE_STATE and not any(
        item.kind is EvidenceKind.CURRENT_TASK_QUEUE for item in cited
    ):
        return (
            _violation(
                "claim_value_not_supported",
                claim,
                "current queue state requires a timestamped current task-queue snapshot",
            ),
        )
    return ()


def _grade_session_three(
    claim: AnswerClaim, cited: tuple[EvidenceRecord, ...]
) -> tuple[EvaluationViolation, ...]:
    violations: list[EvaluationViolation] = []
    kinds = {item.kind for item in cited}

    if claim.kind is ClaimKind.EXPOSED_COUNT and EvidenceKind.BUILD_ID_SUCCESS in kinds:
        violations.append(
            _violation(
                "build_ids_are_not_exposure",
                claim,
                "BuildIds records successful Workflow Task processing and excludes failed replays",
            )
        )

    if claim.kind is ClaimKind.REPORTED_PROBLEM_COUNT:
        reported = tuple(
            item for item in cited if item.kind is EvidenceKind.TEMPORAL_REPORTED_PROBLEM
        )
        if not _has_matching_exact_count(claim, reported):
            violations.append(
                _violation(
                    "reported_problem_count_not_supported",
                    claim,
                    "an exact reported-problem count needs an exact TemporalReportedProblems query",
                )
            )

    if claim.kind is ClaimKind.CONFIRMED_FAILURE_COUNT and claim.certainty is ClaimCertainty.EXACT:
        histories = tuple(item for item in cited if item.kind is EvidenceKind.HISTORY_INSPECTION)
        if not _has_complete_matching_history_count(claim, histories):
            violations.append(
                _violation(
                    "sample_claimed_as_exact",
                    claim,
                    "an exact confirmed count requires a complete history census of its population",
                )
            )

    if claim.kind is ClaimKind.ESTIMATED_FAILURE_COUNT:
        if claim.certainty is ClaimCertainty.EXACT:
            violations.append(
                _violation(
                    "estimate_claimed_as_exact",
                    claim,
                    "a sample-derived estimate must be labeled as an estimate",
                )
            )

    if claim.kind is ClaimKind.SOURCE_DETAIL:
        history_details = {
            detail
            for item in cited
            if item.kind is EvidenceKind.HISTORY_INSPECTION
            for detail in item.source_details
        }
        if not claim.source_detail or claim.source_detail not in history_details:
            violations.append(
                _violation(
                    "source_detail_not_in_history",
                    claim,
                    "source/branch details must appear in cited Temporal failure history evidence",
                )
            )
    return tuple(violations)


def _grade_session_four(
    claim: AnswerClaim, cited: tuple[EvidenceRecord, ...]
) -> tuple[EvaluationViolation, ...]:
    violations: list[EvaluationViolation] = []
    histories = tuple(item for item in cited if item.kind is EvidenceKind.HISTORY_INSPECTION)

    if claim.kind is ClaimKind.CONFIRMED_INCIDENT_COUNT:
        if claim.certainty is ClaimCertainty.EXACT and not _has_complete_matching_history_count(
            claim, histories
        ):
            violations.append(
                _violation(
                    "incident_candidates_are_not_confirmed",
                    claim,
                    "an indexed incident-window candidate count is not a confirmed impact census",
                )
            )

    if claim.kind in {ClaimKind.TEAM_BREAKDOWN, ClaimKind.MONEY_BREAKDOWN}:
        required_field = "Team" if claim.kind is ClaimKind.TEAM_BREAKDOWN else "Domain"
        grouped = tuple(
            item
            for item in cited
            if item.kind is EvidenceKind.SEARCH_ATTRIBUTE_GROUP
            and item.search_attribute == required_field
        )
        if not grouped:
            violations.append(
                _violation(
                    "classification_not_from_search_attribute",
                    claim,
                    f"{required_field} classification requires that registered search attribute",
                )
            )
        if claim.scope is ClaimScope.CONFIRMED_POPULATION:
            confirmed_group = any(
                item.scope is ClaimScope.CONFIRMED_POPULATION and item.groups == claim.groups
                for item in grouped
            )
            complete_history = any(_history_is_population_census(item) for item in histories)
            if not confirmed_group or not complete_history:
                violations.append(
                    _violation(
                        "uninspected_grouped_as_confirmed",
                        claim,
                        "confirmed grouping requires confirmed-run attributes and "
                        "complete inspection",
                    )
                )

    if claim.kind is ClaimKind.HISTORICAL_QUEUE_STATE:
        if any(item.kind is EvidenceKind.CURRENT_TASK_QUEUE for item in cited):
            violations.append(
                _violation(
                    "current_queue_backdated",
                    claim,
                    "current pollers and backlog cannot establish task-queue state "
                    "during the incident",
                )
            )
    return tuple(violations)


def _has_matching_exact_count(claim: AnswerClaim, evidence: tuple[EvidenceRecord, ...]) -> bool:
    return claim.count is not None and any(
        item.exact and item.count == claim.count for item in evidence
    )


def _has_complete_matching_history_count(
    claim: AnswerClaim, histories: tuple[EvidenceRecord, ...]
) -> bool:
    return claim.count is not None and any(
        _history_is_population_census(item) and item.matched_count == claim.count
        for item in histories
    )


def _history_is_population_census(item: EvidenceRecord) -> bool:
    return (
        item.complete
        and item.population_count is not None
        and item.inspected_count == item.population_count
    )


def _violation(code: str, claim: AnswerClaim, message: str) -> EvaluationViolation:
    return EvaluationViolation(code=code, claim_id=claim.claim_id, message=message)
