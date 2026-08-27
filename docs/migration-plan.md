# Migration plan for the current five tools

## Goal

Move the Session 1 prototype onto the generalizable architecture without accidentally preserving
demo heuristics as product semantics. The current demo and live Temporal fixture remain regression
assets, but old dictionary response shapes and tool names are not compatibility requirements unless
the contract review approves them.

Current coupling to remove:

- `server.py` combines MCP adapters, process globals, demo identity, authorization, and error
  flattening.
- `TemporalGateway` combines SDK client creation, visibility, history, task-queue reads, service
  orchestration, and response construction.
- `analysis.py` mixes reusable event decoding with opinionated diagnosis/reset policy.
- limits are module constants rather than one request-scoped execution budget.
- most errors become untyped `ToolError` strings.

## Coordination gate

Before runtime migration, the contract lead must approve—or explicitly mark provisional—the five
capability contracts:

- target resolution;
- workflow filtering;
- workflow counting;
- single-workflow diagnosis;
- reset planning.

At minimum, the gate must settle enough structure to define public model boundaries, completeness,
error mapping, and refusal behavior. Diagnosis confidence and reset recommendation may remain
provisional, but gateways must not encode those policies.

## Dependency-ordered steps

### 1. Record the baseline

- Capture the five current MCP schemas, unit tests, Session 1 integration behavior, and demo trace.
- Add a compatibility manifest for the pinned CLI/dev-server, Python SDK, MCP SDK, Python runtime,
  namespace, and `OrderId` search attribute.
- Label current diagnosis and reset outputs as prototype behavior, not accepted semantics.

This creates a comparison point without making every current field a promise.

### 2. Introduce shared types and protocols

Add the approved public contract models plus conceptual shared interfaces from
`docs/architecture.md`:

- `RequestContext`, `Principal`, and request-context factory;
- `TargetCatalog`, `ResolvedTarget`, and private connection descriptor;
- basic `ExecutionBudget` and accounting snapshot;
- typed internal errors;
- initial visibility, workflow, history, and task-queue fact models;
- gateway and client-provider protocols.

Use fakes for each protocol so downstream work can proceed before concrete gateways are migrated.
Do not make internal fact models aliases of current response dictionaries.

### 3. Extract the Temporal SDK client provider

- Move connection construction and reuse out of `TemporalGateway`.
- Key reuse by an immutable connection/credential generation, not a public target response.
- Make concurrent first acquisition safe.
- Define provider startup, invalidation, and shutdown behavior.
- Keep addresses, TLS, credentials, and future data converters server-side.

Verify client reuse and missing-credential errors without making Temporal calls.

### 4. Split read gateways

Migrate SDK calls and SDK-to-fact conversion into:

- `VisibilityGateway` for list and count;
- `WorkflowGateway` for describe;
- `HistoryGateway` for bounded history retrieval;
- `TaskQueueGateway` for pollers and backlog.

Each gateway accepts the request context/budget, obtains clients only from the provider, and returns
typed facts. Preserve opaque page-token handling in the visibility gateway or a shared pagination
utility. Replace `MAX_HISTORY_EVENTS` with budget consumption and contract-approved limits.

Gateway integration tests assert API requests, pagination, and fact conversion—not diagnosis or
public MCP response wording.

### 5. Separate event facts from reasoning

Split current `analysis.py` into two kinds of components:

- deterministic history decoding that turns events and failure chains into typed facts;
- provisional analysis/recommendation components for diagnosis and reset planning.

Move `likely_cause` and reset-point recommendation behind explicit analysis protocols. Preserve
the Session 1 heuristic only as a provisional implementation with tests that identify its supported
history shape. It must be replaceable without changing gateways or MCP adapters.

### 6. Add tool services

Implement five services against interfaces:

- target-resolution service uses only the target catalog;
- workflow-filter and workflow-count services use the visibility gateway;
- diagnosis service orchestrates workflow, history, and task-queue gateways plus diagnosis
  analysis;
- reset-planning service orchestrates workflow/history facts plus reset analysis.

Every target-bound service reauthorizes `target_ref`. Services assemble the approved response and
completeness/accounting metadata. They do not import FastMCP, raw protobuf types, or concrete SDK
clients.

### 7. Thin the MCP adapters and add a composition root

- Make each registered MCP function validate an approved request, create request context, invoke
  one service, and serialize its response.
- Replace `demo_principal()` calls inside tools with the request-context factory.
- Replace the module-global concrete gateway with dependencies built by one composition root.
- Translate typed errors to stable MCP errors at the adapter boundary.
- Use the same composition root for stdio and streamable HTTP.

Keep the demo principal factory for now. Do not add Cognito in this migration.

### 8. Run vertical acceptance before adding tools

For each existing capability, verify:

- adapter contract tests;
- service tests with fake gateways and budgets;
- gateway integration tests against the pinned dev server;
- pure diagnosis/reset analysis tests over deterministic histories;
- Session 1 through an actual MCP-connected agent CLI;
- budget exhaustion, cancellation, pagination, truncation, authorization, and redaction cases.

The migration is complete when no service or analysis component directly imports the Temporal
client, no gateway returns a public tool dictionary, and no MCP adapter contains Temporal logic.

### 9. Remove the compatibility shell

After the vertical acceptance suite passes:

- delete the old catch-all `TemporalGateway` and unused helpers;
- remove duplicate dataclasses/dictionaries superseded by approved contracts and fact models;
- update the README and demo commands to the new composition root;
- retain the old behavior snapshot as historical evidence, not a second implementation.

## Parallel work and merge dependencies

After the coordination gate, work can proceed in this order:

```text
approved/provisional contracts
          ↓
shared types + protocols + fakes
          ↓
client provider
          ↓
visibility gateways ──┐
workflow/history/queue gateways ─┼─→ tool services → MCP adapters
analysis extraction ───────────┘
          ↓
vertical acceptance and cleanup
```

Visibility and workflow gateway work can run in parallel once provider and fact protocols are
stable. Analysis can progress against history fixtures and gateway fakes. Changes to shared public
contracts, fact models, request context, or budget protocols require review from all downstream
owners and an update to affected scenario assertions.

## Basic versus advanced budgeting

Only the basic budget is required to finish this five-tool migration: deadline/cancellation, calls,
pages, history events, output size, and accounting. Implement the interfaces so reservations and
snapshots are concurrency-safe.

Do not build sampling or a general fan-out engine yet. Add the advanced budget immediately before
`find_workflow_task_failures` and `find_stuck_workflows`, when their approved contracts define
candidate, sample, partial-result, and inspection-report semantics.

## Guardrails

- Do not preserve `likely_cause`, `worth_retrying`, `expires_at`, valid-reset-point selection, or
  command-preview behavior merely because current tests assert them. The contract lead decides.
- Do not move current public dictionaries wholesale into Pydantic and call that a contract review.
- Do not let gateways return conclusions such as “workers are healthy” without the observation
  time and exact queue type represented as facts.
- Do not claim Temporal Cloud or multi-version compatibility based on the dev-server tests.
- Do not mix demo worker/fixture lifecycle with MCP server lifecycle; the fixture is independent
  test infrastructure.
- Do not add authentication, write credentials, or reset execution during this migration.

## Exit criteria

- The five approved/provisional capabilities use the canonical layer boundaries.
- The client provider is the only client-construction path; gateways are the only Temporal-call
  path.
- All calls share one request-scoped basic budget and report approved completeness/accounting.
- Public contracts, internal facts, and analysis results are distinct typed models.
- Diagnosis and reset heuristics are isolated, evidence-backed, and replaceable.
- The pinned Temporal integration suite and Session 1 MCP-agent scenario pass.
- Remaining contract ambiguities are recorded explicitly rather than encoded in architecture.
