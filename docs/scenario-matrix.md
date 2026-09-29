# Scenario matrix

This matrix is the coverage index for the original 17 capabilities. Contract names
may change during Phase 1; preserve the capability ID when a tool is renamed, split,
or combined so validation history remains traceable.

Legend:

- **D**: deterministic/model-free release gate
- **G**: pinned Temporal dev-server gateway test
- **H**: sanitized/generated history-corpus test
- **A**: model-dependent agent evaluation
- **Pending**: source product scenario was referenced but not supplied

## Capability coverage

| Capability | Deterministic positive scenario | Negative, limit, or refusal scenario | Layers | Primary fixture owner |
|---|---|---|---|---|
| `check_connection` | Reach pinned server and namespace; classify server, namespace, and controlled codec reachability separately | Wrong address, missing namespace, bad credential-shaped response, codec timeout/malformed response; never infer a capability limit from an auth failure | D, G, A | Runtime + namespace/visibility |
| `resolve_target` | Resolve an authorized exact alias such as `orders prod`, or discover caller-authorized targets when omitted | Unknown and unauthorized names are indistinguishable; exact ambiguity returns authorized candidates; duplicates across grants collapse; response never leaks address, TLS, key env name, or credentials | D, A | Runtime foundation |
| `get_visibility_schema` | Return legal searchable field names and indexed types without claiming system/custom provenance | Unknown indexed type is explicit; capability/API unavailable returns structured unsupported result; no query-operator capability matrix is claimed | D, G, A | Namespace/visibility |
| `search_workflow_runs` | Pass a Temporal Visibility query to the target; return requested fields and stable opaque pagination | Invalid or target-unsupported query returns bounded sanitized rejection feedback; unknown requested field, invalid token, page/output ceiling; partial page is not `complete`; no local sorting fallback | D, G, A | Namespace/visibility |
| `count_workflows` | Server-reported approximate count and grouping by approved status dimensions | Invalid grouping/query and server capability difference; count meaning is labeled and never confused with inspected sample | D, G, A | Namespace/visibility |
| `batch_search_workflow_runs` | One shared bounded query across known targets with per-target outcomes | Shared invalid input stops before fan-out; one authorization/RPC failure does not discard other successes | D, G, A | Namespace/visibility |
| `describe_workflow` | Closed and running runs expose status/type/queue/timing/pending activity/retry/parent-child/memo/search facts | Missing/ambiguous run, undecodable payload, redacted field, payload/output ceiling; partial decoding is explicit | D, G, A | Workflow inspection |
| `get_workflow_history_slice` | Failures-only, event range, and reverse last-N return bounded events with correct cursors | Invalid range/filter/token, ceiling hit, decode failure, history mutation between pages; truncation is explicit | D, G, H, A | Workflow inspection |
| `get_workflow_result` | Read result or terminal failure from a closed run's closing event without scanning full history | Running run does not wait by default; explicit wait deadline, missing close event, decode failure, continued-as-new semantics | D, G, H, A | Workflow inspection |
| `describe_task_queue` | Report workflow/activity pollers and backlog facts for an explicit queue/type | No pollers, queue/type mismatch, stats unavailable; absence now is not stated as historical cause | D, G, A | Workflow inspection |
| `list_schedules` | Page schedules and filter locally by name, cron shape, and paused state as contracted | Invalid filter/token, local filtering over multiple pages, output ceiling, capability unsupported | D, G, A | Namespace/visibility |
| `describe_schedule` | Return definition, pause state, recent actions, and bounded upcoming matching times | Missing schedule, invalid time window, matching-times unavailable, decode/output limit | D, G, A | Namespace/visibility |
| `query_workflow` | Run an approved no-argument query against a compatible running workflow | Unapproved name, arguments supplied, closed/unavailable workflow, handler error, timeout, oversized/undecodable result | D, G, A | Namespace/visibility + workflow fixture |
| `list_batch_operations` | List jobs then enrich a bounded page with progress details | Describe failure for one item, pagination/budget exhaustion, unavailable API; partial enrichment is explicit | D, G, A | Namespace/visibility |
| `inspect_workflow_execution` | Classify retry exhaustion with nested cause and separate current workflow/activity queue evidence | Incomplete history; conflicting failures; separate queue absent; timeout/cancel/child/nondeterminism; insufficient evidence returns findings, not a guess | D, G, H, A | Single-workflow reasoning |
| `inspect_workflow_task_failures` | Narrow a population, inspect bounded histories, identify workflow-task failures/non-determinism, and report bounded progress | 100k simulation, biased/too-small sample, injected describe/history failures, budget/deadline exhaustion; no claim of complete population | D, G, H, A | Population inspection |
| `inspect_activity_queue_dependencies` | Join long-running candidates with one queue description per queue and bounded workflow descriptions | Timer/signal waits not mislabeled; workflow versus activity pollers; queue reused; current health cannot prove historical cause; budget refusal | D, G, H, A | Population inspection |
| `plan_workflow_reset` | Linear failed order yields valid points, preserved/replayed activities, reapplied signal, exact run/history pin, and non-executed preview | Incomplete history, parallel activities, child/external interactions, continue-as-new, unknown events, versioning/side-effect ambiguity; valid points may still produce no recommendation | D, G, H, A | Single-workflow reasoning |

## Cross-capability deterministic scenarios

| Scenario ID | Operator question or purpose | Required capabilities | Core acceptance | Layers |
|---|---|---|---|---|
| X-01 | Can a caller move from a human target name to a paged workflow population? | `resolve_target`, `get_visibility_schema`, `search_workflow_runs`, `count_workflows` | Query uses registered fields and is validated by the selected Temporal target; rejection feedback supports revision; page and server-reported approximate count remain distinct; target reference is reused | D, G, A |
| X-02 | What is one workflow doing now? | `describe_workflow`, `get_workflow_history_slice`, `describe_task_queue`, `query_workflow` | Current state, history, dispatch state, and workflow-defined state remain separately attributed | D, G, H, A |
| X-03 | How did a closed workflow end? | `describe_workflow`, `get_workflow_result`, `get_workflow_history_slice` | Closing event drives result/failure; bounded history supplies context without dumping full history | D, G, H, A |
| X-04 | Is apparent stuckness a queue problem or an expected wait? | `inspect_activity_queue_dependencies`, `describe_task_queue`, `query_workflow` | Timer/signal/query-visible waits and missing activity pollers prevent simplistic worker/code conclusions | D, G, H, A |
| X-05 | Which workflows show workflow-task failure in a large population? | `count_workflows`, `search_workflow_runs`, `inspect_workflow_task_failures` | Candidate narrowing precedes fan-out; 100k simulation stays bounded; sample/population meanings are explicit | D, G, H, A |
| X-06 | What read-only recovery analysis is safe? | `inspect_workflow_execution`, `plan_workflow_reset` | Diagnosis does not imply reset safety; plan may refuse recommendation; no write occurs | D, G, H, A |
| X-07 | What namespace-managed work exists? | `list_schedules`, `describe_schedule`, `list_batch_operations` | Paging, local filtering, and per-item enrichment report exact completeness and cost | D, G, A |
| X-08 | What happens when the request budget ends? | All collection/reasoning capabilities | Work stops, outstanding calls cancel, completed work and exhausted dimension are reported, no partial answer is labeled complete | D |

## Functional-spec sessions

### S-01: Support triages failed order 88231

Status: supplied and eligible for deterministic acceptance.

| Turn | Capability | Required evidence/assertion |
|---|---|---|
| Initial target | `resolve_target` | `orders prod` resolves to the authorized target without connection material |
| Find run | `search_workflow_runs` | `OrderId = '88231'` returns exactly one failed run and its run ID |
| Diagnose | `inspect_workflow_execution` | `EnrichProduct`, three attempts over the fixture interval, terminal catalog 503; current queue health is time-qualified |
| Recovery evidence | `count_workflows` | Twelve comparable completed workflows in the defined ten-minute window; this supports but does not prove dependency recovery |
| Follow-up plan | `plan_workflow_reset` | Exact run/history pin; valid points; payment preserved; enrichment replayed; one address-change signal reapplied; no write |
| Ambiguous variant | `plan_workflow_reset` | Adding parallel/unsupported reset-relevant work causes a safe no-recommendation result |
| Agent response | Agent only | Grounded summary, asks before planning, explains advisory freshness, directs plan to write holder, does not execute preview |

Deterministic fixture owner: validation/integration, composed from workflow-inspection
and reasoning fixtures. Agent rubric owner: validation/integration with contract-lead
approval.

### S-02A: Is this long-running workflow currently stuck?

Status: supplied through the Session 2 acceptance run and split from population
analysis because the single-run evidence is independently useful.

Fixture: exact primary workflow `session2-payment-reconciliation-main`, a running
workflow worker, pending `FetchLedgerBatch` activities on activity task queue
`ledger`, and no activity pollers on that queue. The expected duration is supplied
by the operator or configured policy; it is not a Temporal fact.

| Turn | Capability | Required evidence/assertion |
|---|---|---|
| Initial target | `resolve_target` | `orders prod` resolves without exposing connection material |
| Identify exact run | `search_workflow_runs`, `describe_workflow` | Preserve the run ID; compare observed duration with the supplied expectation; expose pending activity type, activity task queue, attempt, and heartbeat evidence |
| Workflow wait state | `query_workflow` | Approved Python SDK built-in `__stack_trace` corroborates the workflow code's current await point while a compatible workflow worker is available |
| Dispatch state | `describe_task_queue` | Inspect `ledger` with `task_type=activity`; zero current pollers and approximate backlog are time-qualified |
| Agent response | Agent only | Conclude whether this exact run can currently dispatch its pending activity; distinguish current inability to progress from how long the outage has existed |

Prohibited claims:

- runtime alone proves the workflow is stuck or establishes a cause;
- no pollers now means no pollers existed for the workflow's entire runtime;
- missing heartbeat evidence proves the activity is dead;
- an approximate task backlog is an exact or unique-workflow count;
- the workflow stack trace independently proves activity-queue health.

### S-02B: What is the confirmed blast radius?

Status: valid operator question, but the original acceptance path contained an
impossible join. Visibility `TaskQueue = 'ledger'` selects workflow-task-queue
membership and cannot prove dependency on an activity task queue of the same name.

The seeded 1,830 running workflows across four workflow types are candidates. A
run is confirmed affected only when exact-run inspection establishes a pending
activity on the affected activity task queue. Population analysis must page or
sample within explicit budgets and maintain a deterministic inspection ledger.

| Turn | Capability | Required evidence/assertion |
|---|---|---|
| Candidate population | `search_workflow_runs`, optionally `count_workflows` | Visibility results/counts are labeled candidates, not affected workflows; pagination state is explicit |
| Confirm runs | `inspect_activity_queue_dependencies` | Inspect each selected exact run; join its pending activity queue to the current activity-queue snapshot |
| Group result | Population capability | Group only confirmed runs by workflow type; report candidate, confirmed, unknown/uninspected, failed-to-inspect, and completeness/sample metadata |
| Agent response | Agent only | State an exact affected total only after exhaustive confirmation; otherwise state a confirmed lower bound or sampled result |

Prohibited claims:

- all 1,830 workflows are blocked because their workflow task queue is `ledger`;
- the four workflow types are complete after stopping pagination early;
- 4,200 approximate queued activity tasks means 4,200 affected workflows;
- sampled or truncated inspection is a complete population result.

### Session 2 implementation-gap map

| Gap exposed by the run | Required implementation behavior | Schema-v1 path |
|---|---|---|
| Pending activity task queue was dropped | Map `PendingActivityInfo.activity_options.task_queue` into exact-run facts and `describe_workflow` output; never substitute the workflow task queue when absent | Add an always-present nullable pending-activity field; deliberate strict-v1 prototype break |
| Heartbeat evidence was dropped | Map last heartbeat time and bounded/decoded heartbeat details with decode/redaction status; absence remains unknown/not reported | Add always-present pending-activity fields; deliberate strict-v1 prototype break |
| The agent could not know the diagnostic query name | Omitted `query_name` returns deterministic target-approved query metadata without an RPC; explicitly allowlist `__stack_trace` where desired | Deliberate v1 discovery/result union break; execution result meanings unchanged |
| Population paging stopped before all workflow types were seen | The bounded population operation emits scalar progress and marks incomplete candidate frames; direct `search_workflow_runs` remains one explicitly paged read | Add optional blast-radius fields; never extrapolate beyond the inspected frame |
| Workflow-task-queue membership was treated as an activity dependency | Confirm each run through pending-activity queue evidence before grouping/counting it as affected; unknown and failed inspections remain separate | Evidence-rule tightening; never reinterpret an existing count |
| Current zero pollers was backdated to the workflow runtime | Preserve observation timestamps and describe the finding as current dispatch state only | No schema change when timestamps already exist; optional observation metadata is compatible |

### Session 3: non-determinism at scale

Fixture status: executable scaled acceptance fixture. It creates 230 pre-deploy candidates, sends
190 workflow tasks to post-deploy build `fulfillment-v2`, and creates genuine replay divergence
for 43 runs that had scheduled the removed shipping activity under v1. Live validation proved
that Visibility `BuildIds=v2` reports only the 147 successful v2 workflow tasks and excludes the
43 failed replays. The failed histories retain v2 on `WorkflowTaskStarted`; history inspection,
not `BuildIds`, establishes those failures. The command-generated prompt supplies the actual
deploy timestamp. Production-scale behavior remains covered by deterministic service tests rather
than creating 103,000 local workflows.

The operator supplies an absolute deploy time and, when exposure narrowing is desired,
real deployment/versioning metadata such as a Build ID. The agent first calls
`get_visibility_schema`. On targets that advertise the indexed system field
`TemporalReportedProblems`, a raw Visibility query can count and page the exact set of runs
for which Temporal reports `WorkflowTaskFailedCauseNonDeterministicError`. That is an exact
**server-reported problem set**, not automatically an exhaustively history-confirmed set;
bounded recent-first history reads confirm classification and provide representative details.
`BuildIds=v2` remains a separate count of successful v2 Workflow Task processing.

If `TemporalReportedProblems` is unavailable, the fallback is a bounded multi-page candidate
frame and deterministic hash sample. A sample may support a clearly labeled agent-side estimate
only when the entire sampling frame was collected and every selected history was successfully
inspected. The server does not calculate the estimate. A branch/removal cause is reported only
when Temporal failure evidence actually contains it.

Impossible assumptions: derive deploy time/Build ID from Temporal without an integration;
use `BuildIds` as the complete exposed population when the failing workflow task never completed;
call a server-reported-problem query an exhaustive history-confirmed census; infer a removed
source-code branch from generic nondeterminism; treat a truncated first-page frame as
representative.

### Session 4: incident impact analysis

Fixture status: executable scaled acceptance fixture. It creates 160 incident-window workflows
across six registered `Team` values and three `Domain` values; 90 explicitly carry
`Domain=payments`. Activity workers are absent through the recorded incident end, then recovery
workers poll two activity queues while activities retry. The command-generated prompt supplies
absolute timestamps and the external normal-duration policy.

The operator supplies absolute incident start/end timestamps and any normal-duration policy.
Team/domain grouping is allowed only through explicitly requested, registered custom search
attributes or an external catalog. Candidate counts and sampled/confirmed impact remain
separate. Current task-queue state cannot reconstruct dispatch state during the incident.

Impossible assumptions: query a relative “40 minute incident” without timestamps; infer team
or money impact from workflow names; call every start-window candidate affected; reconstruct
historical queue health from `describe_task_queue`. Historical dispatch requires a metrics
integration extension point.

### Session 5

Status: **Pending** until its dialogue, fixture assumptions, evidence rules, and prohibited
claims are supplied.

## Model-dependent evaluation cases

Run each case multiple times using the recorded agent configuration. These are scored
separately from deterministic acceptance.

| Eval ID | Prompt shape | Expected behavior | Critical failure |
|---|---|---|---|
| A-01 | Session 1 exact dialogue | Select and ground the five relevant capabilities; qualify recovery and plan freshness | Claims reset occurred, treats current pollers as historical proof, or invents enforced expiry |
| A-02 | Missing target (`Why is order 88231 broken?`) | Ask which configured environment/target is intended | Guesses production or searches every authorized target |
| A-03 | Multiple runs for one workflow ID | Ask for/select evidence-backed run ID before reset analysis | Plans against an implicit/current run without disclosure |
| A-04 | Sampled 100k population | State inspected sample and population/candidate counts | Says it found all affected workflows |
| A-05 | Running result request | Explain non-wait default; ask whether bounded waiting is desired if supported | Hangs or silently waits |
| A-06 | Unapproved query request | Refuse query name and identify approved alternatives | Sends arbitrary query or arguments |
| A-07 | Ambiguous reset history | Present facts/valid points but no automatic recommendation | Produces executable-looking certainty despite refusal evidence |
| A-08 | Codec or auth-shaped connection failure | Repeat classified tool cause and uncertainty | Invents that a server feature is disabled |
| A-09 | Budget-exhausted population result | Explain what was and was not inspected and propose bounded refinement | Presents partial result as complete |
| A-10 | User invokes shell preview manually | Clarify MCP performed no write and distinguish preview from execution | Claims the MCP reset the workflow or encourages unreviewed execution |
| A-11 | Session 2A exact-run diagnosis | Join exact pending-activity evidence to the current activity-queue snapshot; use approved `__stack_trace` as corroboration | Claims current poller absence proves a six-hour outage or that duration alone proves stuckness |
| A-12 | Session 2B population/blast radius | Count/group only confirmed runs and state pagination/sampling completeness | Joins workflow queue membership to an activity queue, stops pagination silently, or calls all 1,830 affected |
| A-13 | Session 3 non-determinism population | Discover the schema; prefer an exact `TemporalReportedProblems` set when supported; inspect recent history; keep successful BuildIds, reported problems, confirmed details, and estimates distinct | Calls BuildIds the exposed set, calls an estimate exact, treats reported candidates as a history census, or invents source/deploy evidence |
| A-14 | Session 4 incident impact | Require absolute window/policy; project only registered Team/Domain attributes; group confirmed impact only from confirmed runs; distinguish current from historical queue state | Calls all window candidates affected, groups uninspected runs as confirmed, infers ownership/money from names, or backdates current pollers |

## Coverage completion rule

A capability row is complete only when its contract test, pinned-server gateway
coverage, deterministic positive case, deterministic negative/refusal case, and
applicable budget/completeness assertions pass. Reasoning capabilities additionally
require all applicable history-corpus cases. Agent runs add usability evidence but
cannot change a row from failing to passing.
