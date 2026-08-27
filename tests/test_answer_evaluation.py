from ops_mcp.evaluation import (
    AnswerClaim,
    AnswerManifest,
    ClaimCertainty,
    ClaimKind,
    ClaimScope,
    EvaluationScenario,
    EvidenceKind,
    EvidenceRecord,
    grade_answer,
)


def _evidence(kind: EvidenceKind, evidence_id: str, **kwargs: object) -> EvidenceRecord:
    return EvidenceRecord(
        evidence_id=evidence_id,
        kind=kind,
        description=evidence_id,
        scope=kwargs.pop("scope", ClaimScope.CANDIDATE_POPULATION),
        **kwargs,
    )


def _claim(kind: ClaimKind, claim_id: str, evidence_id: str, **kwargs: object) -> AnswerClaim:
    return AnswerClaim(
        claim_id=claim_id,
        kind=kind,
        statement=claim_id,
        certainty=kwargs.pop("certainty", ClaimCertainty.EXACT),
        scope=kwargs.pop("scope", ClaimScope.CANDIDATE_POPULATION),
        evidence_ids=(evidence_id,),
        **kwargs,
    )


def _codes(manifest: AnswerManifest) -> set[str]:
    return {item.code for item in grade_answer(manifest).violations}


def test_session_three_accepts_exact_reported_problem_count_and_history_details() -> None:
    reported = _evidence(
        EvidenceKind.TEMPORAL_REPORTED_PROBLEM,
        "reported",
        scope=ClaimScope.REPORTED_PROBLEM_POPULATION,
        exact=True,
        complete=True,
        count=43,
        cause="WorkflowTaskFailedCauseNonDeterministicError",
        workflow_ids=("run-1", "run-2"),
    )
    history = _evidence(
        EvidenceKind.HISTORY_INSPECTION,
        "history",
        scope=ClaimScope.INSPECTED_SAMPLE,
        population_count=43,
        inspected_count=2,
        matched_count=2,
        source_details=("removed shipping activity command",),
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(reported, history),
        claims=(
            _claim(
                ClaimKind.REPORTED_PROBLEM_COUNT,
                "reported-count",
                "reported",
                scope=ClaimScope.REPORTED_PROBLEM_POPULATION,
                count=43,
            ),
            _claim(
                ClaimKind.SOURCE_DETAIL,
                "detail",
                "history",
                certainty=ClaimCertainty.QUALIFIED,
                scope=ClaimScope.INSPECTED_SAMPLE,
                source_detail="removed shipping activity command",
            ),
        ),
    )

    assert grade_answer(manifest).passed


def test_session_three_rejects_build_ids_as_exposed_total() -> None:
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(
            _evidence(
                EvidenceKind.BUILD_ID_SUCCESS,
                "build",
                scope=ClaimScope.BUILD_SUCCESS_POPULATION,
                exact=True,
                count=147,
            ),
        ),
        claims=(
            _claim(
                ClaimKind.EXPOSED_COUNT,
                "exposed",
                "build",
                scope=ClaimScope.BUILD_SUCCESS_POPULATION,
                count=147,
            ),
        ),
    )

    assert "build_ids_are_not_exposure" in _codes(manifest)


def test_session_three_rejects_sample_as_exact_and_accepts_labeled_estimate() -> None:
    history = _evidence(
        EvidenceKind.HISTORY_INSPECTION,
        "sample",
        scope=ClaimScope.INSPECTED_SAMPLE,
        population_count=1_900,
        inspected_count=200,
        matched_count=43,
    )
    exact = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(history,),
        claims=(
            _claim(
                ClaimKind.CONFIRMED_FAILURE_COUNT,
                "wrong-exact",
                "sample",
                scope=ClaimScope.CONFIRMED_POPULATION,
                count=43,
            ),
        ),
    )
    estimate = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(history,),
        claims=(
            _claim(
                ClaimKind.ESTIMATED_FAILURE_COUNT,
                "estimate",
                "sample",
                certainty=ClaimCertainty.ESTIMATE,
                scope=ClaimScope.CANDIDATE_POPULATION,
                count=409,
            ),
        ),
    )

    assert "sample_claimed_as_exact" in _codes(exact)
    assert grade_answer(estimate).passed


def test_session_three_rejects_estimate_with_exact_certainty() -> None:
    history = _evidence(
        EvidenceKind.HISTORY_INSPECTION,
        "sample",
        scope=ClaimScope.INSPECTED_SAMPLE,
        population_count=1_900,
        inspected_count=200,
        matched_count=43,
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(history,),
        claims=(
            _claim(
                ClaimKind.ESTIMATED_FAILURE_COUNT,
                "exact-estimate",
                "sample",
                scope=ClaimScope.CANDIDATE_POPULATION,
                count=409,
            ),
        ),
    )

    assert "estimate_claimed_as_exact" in _codes(manifest)


def test_session_three_rejects_source_detail_not_present_in_history() -> None:
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(
            _evidence(
                EvidenceKind.HISTORY_INSPECTION,
                "history",
                scope=ClaimScope.INSPECTED_SAMPLE,
                population_count=10,
                inspected_count=2,
                matched_count=1,
                source_details=("command mismatch",),
            ),
        ),
        claims=(
            _claim(
                ClaimKind.SOURCE_DETAIL,
                "invented",
                "history",
                certainty=ClaimCertainty.QUALIFIED,
                scope=ClaimScope.INSPECTED_SAMPLE,
                source_detail="shipping.py:81 removed branch",
            ),
        ),
    )

    assert "source_detail_not_in_history" in _codes(manifest)


def test_session_four_rejects_candidate_count_as_confirmed() -> None:
    manifest = AnswerManifest(
        scenario=EvaluationScenario.INCIDENT_IMPACT,
        evidence=(
            _evidence(
                EvidenceKind.VISIBILITY_CANDIDATES,
                "candidates",
                exact=True,
                complete=True,
                count=160,
            ),
        ),
        claims=(
            _claim(
                ClaimKind.CONFIRMED_INCIDENT_COUNT,
                "affected",
                "candidates",
                scope=ClaimScope.CONFIRMED_POPULATION,
                count=160,
            ),
        ),
    )

    assert "incident_candidates_are_not_confirmed" in _codes(manifest)


def test_session_four_rejects_uninspected_team_groups_as_confirmed() -> None:
    teams = _evidence(
        EvidenceKind.SEARCH_ATTRIBUTE_GROUP,
        "teams",
        scope=ClaimScope.CANDIDATE_POPULATION,
        exact=True,
        complete=True,
        search_attribute="Team",
        groups={"treasury": 50, "refunds": 25},
    )
    sample = _evidence(
        EvidenceKind.HISTORY_INSPECTION,
        "sample",
        scope=ClaimScope.INSPECTED_SAMPLE,
        population_count=160,
        inspected_count=30,
        matched_count=20,
    )
    claim = AnswerClaim(
        claim_id="team-impact",
        kind=ClaimKind.TEAM_BREAKDOWN,
        statement="team-impact",
        certainty=ClaimCertainty.EXACT,
        scope=ClaimScope.CONFIRMED_POPULATION,
        evidence_ids=("teams", "sample"),
        groups=teams.groups,
        search_attribute="Team",
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.INCIDENT_IMPACT,
        evidence=(teams, sample),
        claims=(claim,),
    )

    assert "uninspected_grouped_as_confirmed" in _codes(manifest)


def test_session_four_requires_team_and_money_search_attributes() -> None:
    candidates = _evidence(
        EvidenceKind.VISIBILITY_CANDIDATES,
        "names",
        exact=True,
        complete=True,
        count=90,
    )
    team_claim = _claim(
        ClaimKind.TEAM_BREAKDOWN,
        "teams-from-names",
        "names",
        groups={"treasury": 50},
        search_attribute="Team",
    )
    money_claim = _claim(
        ClaimKind.MONEY_BREAKDOWN,
        "money-from-names",
        "names",
        groups={"payments": 90},
        search_attribute="Domain",
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.INCIDENT_IMPACT,
        evidence=(candidates,),
        claims=(team_claim, money_claim),
    )

    assert _codes(manifest) == {"classification_not_from_search_attribute"}


def test_session_four_rejects_current_queue_snapshot_as_historical() -> None:
    queue = _evidence(
        EvidenceKind.CURRENT_TASK_QUEUE,
        "queue-now",
        scope=ClaimScope.CURRENT_SNAPSHOT,
        observed_at="2026-08-24T11:00:00Z",
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.INCIDENT_IMPACT,
        evidence=(queue,),
        claims=(
            _claim(
                ClaimKind.HISTORICAL_QUEUE_STATE,
                "historical-outage",
                "queue-now",
                scope=ClaimScope.HISTORICAL_WINDOW,
            ),
        ),
    )

    assert "current_queue_backdated" in _codes(manifest)


def test_session_four_accepts_confirmed_census_and_attribute_groups() -> None:
    history = _evidence(
        EvidenceKind.HISTORY_INSPECTION,
        "census",
        scope=ClaimScope.CONFIRMED_POPULATION,
        complete=True,
        population_count=160,
        inspected_count=160,
        matched_count=120,
    )
    teams = _evidence(
        EvidenceKind.SEARCH_ATTRIBUTE_GROUP,
        "teams",
        scope=ClaimScope.CONFIRMED_POPULATION,
        exact=True,
        complete=True,
        search_attribute="Team",
        groups={"treasury": 50, "refunds": 25},
    )
    manifest = AnswerManifest(
        scenario=EvaluationScenario.INCIDENT_IMPACT,
        evidence=(history, teams),
        claims=(
            AnswerClaim(
                claim_id="affected",
                kind=ClaimKind.CONFIRMED_INCIDENT_COUNT,
                statement="affected",
                certainty=ClaimCertainty.EXACT,
                scope=ClaimScope.CONFIRMED_POPULATION,
                evidence_ids=("census",),
                count=120,
            ),
            AnswerClaim(
                claim_id="teams",
                kind=ClaimKind.TEAM_BREAKDOWN,
                statement="teams",
                certainty=ClaimCertainty.EXACT,
                scope=ClaimScope.CONFIRMED_POPULATION,
                evidence_ids=("census", "teams"),
                groups=teams.groups,
                search_attribute="Team",
            ),
        ),
    )

    assert grade_answer(manifest).passed


def test_missing_evidence_id_is_a_deterministic_failure() -> None:
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(),
        claims=(
            _claim(
                ClaimKind.CANDIDATE_COUNT,
                "candidate",
                "not-recorded",
                count=230,
            ),
        ),
    )

    assert _codes(manifest) == {"missing_evidence"}


def test_manifest_round_trips_as_json_without_prose_matching() -> None:
    manifest = AnswerManifest.model_validate(
        {
            "scenario": "session_3_nondeterminism",
            "evidence": [
                {
                    "evidence_id": "candidates",
                    "kind": "visibility_candidates",
                    "description": "pre-deploy query",
                    "scope": "candidate_population",
                    "exact": True,
                    "complete": True,
                    "count": 230,
                }
            ],
            "claims": [
                {
                    "claim_id": "candidate-count",
                    "kind": "candidate_count",
                    "statement": "There are 230 pre-deploy candidates.",
                    "certainty": "exact",
                    "scope": "candidate_population",
                    "evidence_ids": ["candidates"],
                    "count": 230,
                }
            ],
        }
    )

    assert AnswerManifest.model_validate_json(manifest.model_dump_json()) == manifest
    assert grade_answer(manifest).passed


def test_structured_count_must_match_cited_evidence() -> None:
    manifest = AnswerManifest(
        scenario=EvaluationScenario.NONDETERMINISM,
        evidence=(
            _evidence(
                EvidenceKind.VISIBILITY_CANDIDATES,
                "candidates",
                exact=True,
                complete=True,
                count=230,
            ),
        ),
        claims=(
            _claim(
                ClaimKind.CANDIDATE_COUNT,
                "wrong-count",
                "candidates",
                count=103_000,
            ),
        ),
    )

    assert _codes(manifest) == {"claim_value_not_supported"}
