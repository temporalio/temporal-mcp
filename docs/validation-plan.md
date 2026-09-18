# Validation plan for the generalizable POC

## Purpose and scope

This plan validates that the read-only Temporal Ops MCP capabilities answer their
contracted operator questions accurately, within declared bounds, and without
turning model behavior into the definition of correctness.

It covers the 17 capabilities in the supplied functional specification. Session 1
is specified below. Evidence from the Session 2 acceptance run defines two separate
scenarios: 2A diagnoses one exact run and 2B assesses a population. Sessions 3–5
remain blocked on missing source text and must not be invented by an implementer.

Authentication, write operations, Temporal Cloud compatibility, and production
deployment are outside this POC. The target environment is pinned to:

- Temporal CLI `1.7.0`, whose bundled dev server reports `1.31.0`
- Temporal Python SDK `1.31.0`
- MCP Python SDK `1.29.0`

These are the versions in the current workspace as of 2026-08-23. The runtime
foundation owner should record them in one compatibility manifest and make CI fail
on drift. A deliberate upgrade updates the manifest and re-runs the full gateway
suite. Passing this suite does not imply Temporal Cloud compatibility.

## What determines correctness

Validation has two separate tracks.

### Deterministic acceptance

Deterministic tests are the release gate. Given fixed configuration, typed Temporal
facts, serialized histories, and server fixtures, they assert exact contracts,
classification, limits, refusals, and provenance. These tests must pass without an
LLM.

### Agent evaluation

Agent evaluations measure usability: tool selection, argument construction,
follow-up behavior, grounding, and communication of uncertainty. They are
model-dependent observations, not the source of truth for tool semantics. A model
failure may motivate a contract or description change, but it does not justify
weakening a deterministic assertion.

## Test layers

### 1. Contract tests

Owner: contracts/architecture lead, with fixtures supplied by each capability
owner.

Run in process with mocked gateways. Assert for every capability:

- MCP name, description, read-only annotation, and typed input/output schema
- required versus optional arguments and boundary validation
- `schema_version` behavior once Phase 1 decides the versioning convention
- structured error code and safe message for each declared failure
- no address, API key, TLS material, codec secret, or unrequested payload leakage
- completeness state is one of `complete`, `truncated`, `sampled`,
  `budget_exhausted`, or `unknown` whenever a collection is returned
- counts are labeled by meaning: visibility matches, candidates, runs inspected,
  or histories inspected
- facts are distinguishable from findings and recommendations
- evidence, confidence, caveats, and counterevidence appear where the approved
  reasoning contract requires them
- response size and pagination limits are enforced before serialization

Contract tests should compare typed fields and semantic values, not incidental JSON
key order or prose punctuation.

### 2. Temporal gateway integration tests

Owner: the implementer who owns the corresponding gateway. Runtime foundation owns
the shared server fixture and version check.

Run against a fresh `temporal server start-dev` process at the pinned version. Use
a unique namespace where supported, and always use unique workflow IDs and task
queues. Seed only through fixture workflows, activities, schedules, signals, and
queries; never depend on a developer's existing namespace.

These tests establish the actual SDK/server contract for:

- system and namespace reachability
- visibility list/count, grouping, pagination, and custom search attributes
- forward and reverse history pagination
- multi-page population collection before deterministic sampling, including a
  1,900-row frame whose first 200 rows contain only one workflow type
- explicit nonrepresentative metadata for candidate/page-truncated frames and
  estimate eligibility only for complete-frame, fully inspected samples
- recent-first workflow-task failure detection with exact bounded failure evidence
- opt-in Team/Domain projection and bounded aggregation, including unregistered,
  absent, decode-failed/redacted, and output-bound cases
- workflow description, closing events, pending activities, parent/child links,
  memo, and search attributes
- workflow-task and activity-task queue descriptions
- schedules, matching times, approved custom queries, the Python SDK built-in
  `__stack_trace` query, and batch-operation reads
- SDK/server errors for missing runs, invalid visibility queries, unavailable
  features, and deadlines

Gateway tests assert typed Temporal facts only. They do not assert diagnostic or
reset recommendations. Codec connectivity should use a controlled local fake codec
endpoint; it must not require customer infrastructure.

The suite must print the CLI, server, Python SDK, and MCP SDK versions. It should
stop immediately if the server version differs from the manifest.

### 3. History corpus and reasoning tests

Owner: single-workflow reasoning implementer. Population implementer owns corpus
metadata used only for population classification. Contracts lead reviews expected
findings and refusals.

Store sanitized JSON histories exported from the pinned dev server when the exact
wire shape matters. Prefer small deterministic history generators for combinatorial
variations. Every corpus case must declare:

- corpus format version and pinned Temporal version
- fixture/generator name and purpose
- expected facts and event relationships
- expected findings and confidence
- expected caveats or counterevidence
- expected recommendation, or an explicit refusal reason
- whether replay compatibility is relevant

Minimum corpus:

- activity success and retry exhaustion, including nested failure causes
- non-retryable activity failure
- activity start-to-close, schedule-to-close, and heartbeat timeout
- workflow-task failure, including non-determinism and deadlock-shaped failures
- cancellation, termination, and workflow timeout
- healthy running workflow waiting on a timer
- running workflow waiting on a signal or approved query-visible state
- no workflow-task pollers
- workflow-task pollers present but activity-task pollers absent
- separate workflow and activity task queues
- parent and failed child workflow
- parallel activities with mixed completion state
- signal and update events around candidate reset points
- child workflow and external-workflow interaction around a reset point
- continue-as-new chain
- local activity or other unsupported reset-relevant history shape
- code/worker versioning ambiguity
- unknown event enum/type handled without a guessed conclusion
- history exactly at, below, and above the configured ceiling

Reasoning tests consume typed facts or histories and must not call a live Temporal
server. Replay tests may additionally use the Python SDK `Replayer`, but replay
success is not a substitute for reset-impact analysis.

### 4. Functional scenario acceptance

Owner: validation/integration owner. Capability owners supply fixture builders and
expected typed results.

Scenarios call MCP tools through the MCP transport with a scripted, model-free
client. They assert tool order only where the product contract requires it; otherwise
they assert the evidence needed to answer the operator question. Each scenario
records all tool inputs, outputs, budget consumption, and the final deterministic
answer template.

Session 1 must verify:

1. `resolve_target` maps `orders prod` without exposing connection material.
2. `search_workflow_runs` resolves `OrderId = '88231'` to one failed exact run.
3. `inspect_workflow_execution` reports three failed `EnrichProduct` attempts ending in a
   catalog 503, while distinguishing current worker health from historical health.
4. `count_workflows` reports twelve completed comparable workflows in the specified
   recovery window.
5. Any recovery conclusion identifies the count as supporting evidence rather than
   proof that the failed activity is universally safe to retry.
6. `plan_workflow_reset` is pinned to the run and observed history, reports valid
   points and replay impact, and performs no write.
7. Payment authorization is classified as already completed; product enrichment as
   replayed; the address-change signal as reapplied.
8. The planner safely refuses if this same fixture is changed to include an
   unsupported or ambiguous reset condition.
9. Any freshness time is labeled advisory (`recheck_after`) unless an enforcing
   write-side mechanism exists.
10. A command preview, if retained by the approved contract, is explicitly
    non-executed, single-line/shell-safe, and contains no Markdown escaping.

Session 2A must verify:

1. The exact workflow run has exceeded an externally supplied/configured expected
   duration; Temporal runtime alone is not labeled proof of stuckness.
2. `describe_workflow` reports the pending activity's activity task queue, state,
   attempt/max attempts, timestamps, and bounded heartbeat evidence when present.
3. `query_workflow` invokes the explicitly approved Python SDK built-in
   `__stack_trace` query; the fixture does not require a custom `blocked_on` query.
4. `describe_task_queue(task_type="activity")` reports the current poller snapshot
   and approximate backlog for the exact pending activity queue.
5. The deterministic answer may conclude that the exact run cannot currently
   dispatch its pending activity, but it does not backdate the poller outage.
6. Query unavailability, missing heartbeat evidence, or unavailable enhanced queue
   stats remain explicit unknowns; none is converted into a guessed cause.

Session 2B must verify:

1. Visibility `TaskQueue` is treated as workflow-task-queue membership only.
2. Each affected run is confirmed by exact-run pending-activity evidence joined to
   the affected **activity** task queue; equal queue names are not a join key.
3. Pagination/fan-out is exhaustive or explicitly sampled/truncated/budget-exhausted.
4. Results separately report candidate, inspected, confirmed, unknown/uninspected,
   failed-to-inspect, and completeness counts.
5. Workflow-type grouping contains confirmed runs only and is complete only when
   the confirmed population was exhaustively inspected.
6. The approximate activity backlog is never presented as a unique-workflow count.

Session 3 and 4 now have executable scaled fixtures. Their release gates still require a recorded
agent run against a clean Temporal dev namespace using the command-generated timestamps. Session
5 still needs its original text, persona, fixture, expected evidence, and acceptance assertions.

Session 3 must verify the agent answers the high-level operator question by:

1. Calling `get_visibility_schema` before choosing its narrowing query.
2. When `TemporalReportedProblems` is advertised, using a raw Visibility query for
   `WorkflowTaskFailedCauseNonDeterministicError` to count and page the exact server-reported
   problem set, then inspecting a bounded set of those histories for confirmation and details.
3. Separating the 230 pre-deploy candidates, the 147 runs whose v2 Workflow Task completed
   successfully and therefore acquired `BuildIds=v2`, the exact server-reported problem set,
   and the number of histories actually inspected.
4. Never treating `BuildIds` as the exposed set: a nondeterministic v2 replay can fail before
   that build is added to Visibility, even though its `WorkflowTaskStarted` history records v2.
5. When `TemporalReportedProblems` is unavailable, falling back to complete-frame bounded
   sampling and labeling any extrapolation as an estimate rather than an exact count.
6. Not using the fixture's private ground-truth list as evidence, and stating a source branch,
   file, or removed command only when that detail occurs in inspected failure history.

Session 4 must verify the agent answers which teams were affected and which touch money using the
registered `Team` and `Domain` attributes. It must preserve the distinction between window
candidates and confirmed activity evidence, group only confirmed runs as confirmed impact, and
separate historical schedule-to-start evidence from current recovery-worker state.

### 5. Budget, completeness, and fault tests

Owner: runtime foundation for primitive counters/deadlines; population-inspection
owner for fan-out and sampling behavior; each service owner for propagation.

Use deterministic fake gateways with call barriers and injected failures. Cover:

- deadline already expired and deadline expiring mid-pagination
- cancellation propagated to outstanding calls
- API-call, history-event, page, output-size, sample, and concurrency ceilings
- exact-boundary success and one-over-boundary behavior
- deterministic sample selection for a fixed seed/strategy
- candidate narrowing before history fan-out
- one task-queue description reused across candidates on the same queue
- partial page, partial history, and partial fan-out failures
- no silent conversion of a partial answer into `complete`
- `sampled` reports population/candidate/sample sizes and sampling method
- `budget_exhausted` reports the exhausted dimension and work completed
- `truncated` carries a usable continuation only when the contract permits one
- unknown population size stays `unknown`; it is never inferred from a page size
- output ordering is stable under concurrent completion
- no retries that can exceed the request budget or deadline

Add one scale simulation representing 100,000 visibility matches without creating
100,000 live workflows. It must prove bounded list/describe/history calls and stable
inspection metadata. A smaller live-server population verifies API integration; the
large case belongs to deterministic simulation.

### 6. Reproducible agent evaluations

Owner: validation/integration owner. Contracts lead approves the rubric.

Use Codex CLI as the primary recorded demo. Claude Code may be a secondary
exploratory run. Pin and record:

- agent CLI version, model identifier, system/runbook prompt, and sampling settings
- MCP configuration and server commit/version
- Temporal compatibility manifest and fixture ID
- full user turns, tool trace, tool results, final answer, and evaluator rubric

Score, rather than require exact prose:

- chose authorized tools and supplied valid arguments
- resolved the target before target-scoped reads
- used run IDs when ambiguity or reset planning requires them
- grounded each material claim in returned evidence
- distinguished current worker health from worker health at failure time
- communicated sampling, truncation, budget exhaustion, and uncertainty
- did not turn a recommendation into a fact
- did not claim advisory plan expiry is mechanically enforced
- did not execute, encourage automatic execution of, or describe a command preview
  as an MCP write
- asked for missing target/run/contract information instead of guessing

Each evaluation set includes positive cases, ambiguous cases that require a
follow-up question, and refusal cases. Model results are trended across runs; they
do not replace model-free scenario acceptance.

#### Structured evidence/claim grading for Sessions 3 and 4

Recorded answers for these sessions also produce an `AnswerManifest` using
`ops_mcp.evaluation`. This is a JSON-serializable evaluation artifact, not an MCP tool or a
production query filter. Each material claim records its semantic kind, scope, certainty, value,
and cited evidence IDs. Each evidence record identifies whether it came from Visibility, a
history inspection, a current task-queue snapshot, a registered search-attribute grouping, or an
external policy. The deterministic grader evaluates those fields; it never requires exact prose.

Acceptable Session 3 claim shape:

- “43 runs are in Temporal's exact server-reported nondeterminism set” cites an exact
  `TemporalReportedProblems` count and uses `reported_problem_count`.
- “I inspected 10 of those 43 and all 10 histories contain nondeterministic Workflow Task
  failures” cites the bounded history record and keeps its scope `inspected_sample`.
- “147 runs successfully processed a v2 Workflow Task” may cite `BuildIds=v2`, but it is a
  `build_success_count`, not an `exposed_count`.
- A sample extrapolation uses `estimated_failure_count` with certainty `estimate`. A source-level
  detail must exactly occur in the cited history evidence.

Acceptable Session 4 claim shape:

- The incident-window Visibility count is a `candidate_count` until historical evidence confirms
  impact. An exact `confirmed_incident_count` requires a complete history census of its stated
  population; a partial inspection can report observed matches or a lower bound.
- A confirmed team or money breakdown cites both confirmed-run history evidence and grouping by
  the registered `Team` or `Domain` search attribute. Candidate-level grouping remains labeled as
  candidate context.
- Current poller/backlog evidence may support `current_queue_state`; it cannot support a
  `historical_queue_state` claim about the incident window.

The release gate fails deterministically for missing evidence; BuildIds labeled as the exposed
total; a sample estimate labeled exact; source details absent from history; indexed candidates
called confirmed incident impact; uninspected candidates grouped as confirmed; team/domain
classification inferred without the configured attributes; or current queue state backdated to
the incident.

## Expected refusal families

Refusals are successful outcomes when they prevent an unsupported conclusion.
Minimum families are:

- target not found or not authorized
- namespace/server/codec unavailable with classified cause
- invalid visibility syntax or unsupported search operator
- running workflow result requested without explicit wait
- query name not approved, query needs arguments, or workflow is not queryable
- missing exact run where run identity matters
- incomplete history prevents diagnosis or reset analysis
- budget/deadline prevents representative inspection
- no meaningful automatic diagnosis from available Temporal evidence
- reset history contains unsupported events, parallel side effects, child/external
  workflow ambiguity, continue-as-new ambiguity, or versioning risk
- valid reset points exist but recommendation policy cannot choose safely
- population sample is too small or biased for the requested conclusion
- workflow-task-queue membership is the only proposed evidence of an
  activity-task-queue dependency
- capability absent from the pinned target

Every refusal returns the known facts, the missing evidence, and the least-privilege
next step when the contract defines one. It must not fabricate a likely cause.

## Fixture ownership and handoffs

| Fixture | Primary owner | Consumers | Required handoff |
|---|---|---|---|
| Pinned dev-server launcher and manifest | Runtime foundation | All gateway/scenario suites | Health check, isolated namespace rules, teardown, version report |
| Target catalog fixtures | Runtime foundation | Contract and scenario suites | Authorized, ambiguous, missing, and unauthorized aliases |
| Visibility population builder | Namespace/visibility implementer | Population and scenario suites | Search attributes, exact expected counts/groups, pagination order |
| Workflow/activity fixture library | Workflow-inspection implementer | Gateway, reasoning, scenario suites | IDs, queues, lifecycle controls, expected raw facts |
| Schedule/query/batch fixtures | Namespace/visibility implementer | Gateway and capability scenarios | Capability prerequisites and cleanup contract |
| Sanitized histories/generators | Single-workflow reasoning implementer | Diagnosis/reset tests | Expected findings/refusals and provenance |
| Fan-out fake gateway and 100k simulation | Population implementer | Budget and population tests | Call ledger, fault injection, deterministic sampling |
| Functional-session seeds | Validation/integration owner | Model-free and agent tests | Immutable fixture ID, expected evidence, reset recipe |
| Agent transcripts and rubric results | Validation/integration owner | Product review | Reproduction metadata and separation from deterministic results |

Fixture changes that alter expected product behavior require a contract/decision-log
review. Fixture-only implementation changes that preserve facts do not.

## Minimum exit criteria

The generalizable POC is validation-complete when:

1. All 17 approved capabilities have passing contract tests and at least one
   deterministic positive and negative scenario.
2. Every underlying Temporal gateway call is exercised against the pinned self-hosted
   dev server, or the capability is explicitly marked unsupported/deferred.
3. The required history corpus passes with evidence, confidence, caveats, and safe
   refusals; there are no unclassified corpus cases.
4. Every collection/fan-out path passes limit, completeness, cancellation, and
   partial-failure tests.
5. Sessions 1, 2A, and 2B pass end to end through MCP. Sessions 3–5 pass only after
   their missing source definitions are supplied and approved; until then the
   five-session product acceptance milestone remains incomplete.
6. The 100,000-match simulation proves bounded work and honest sampling metadata.
7. Agent evaluations are reproducible and show no systematic unsupported claim,
   silent sampling, or write-action confusion across the agreed run count.
8. Test output records the compatibility manifest, and no result claims Temporal
   Cloud support.
9. Every discovered semantic mismatch updates the capability contract and product
   decision log before the implementation is considered done.
