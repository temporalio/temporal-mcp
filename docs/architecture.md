# Generalizable POC architecture

## Purpose and scope

This architecture supports the approved read-only Temporal operations tools while the team
validates their product contracts. It deliberately leaves authentication, write operations,
production audit storage, and customer-specific runbook policy for later. Those concerns have
extension points, but they must not distort this proof of concept.

Tool names, public response fields, confidence semantics, refusal rules, and reset semantics are
product-contract decisions. This document defines where approved behavior belongs; it does not
settle those decisions.

## Canonical vocabulary

Use this vocabulary in code, documentation, and reviews:

```text
MCP transport adapters
        ↓
Tool services
        ↓
Optional analysis/recommendation components
        ↓
Temporal gateways
        ↓
Temporal SDK client provider
```

The arrows show allowed dependency direction, not a requirement that every request traverse every
layer. A direct-read service usually calls a gateway directly. A reasoning service uses analysis
components to interpret typed facts returned by one or more gateways.

There is no generic **API layer**. Earlier uses of that phrase meant **Temporal gateways**: the
components that call Temporal APIs. The **Temporal SDK client provider** only supplies configured
SDK clients; it does not implement workflow, visibility, history, or task-queue operations.

The following concerns cross the layers:

- **Request context:** caller identity seam, request/trace ID, deadline, and execution budget.
- **Target catalog:** target definitions, aliases, and group grants; authorization remains enforced
  when a target reference is consumed, not only when it is resolved.
- **Execution budget:** common bounds and accounting for calls, pages, events, time, and output.
- **Typed contracts:** public MCP models, internal Temporal facts, findings, inspection metadata,
  and errors.

## Layer responsibilities

### MCP transport adapters

- Register MCP tools and their read-only annotations.
- Parse and validate transport-level arguments using approved public request models.
- Create a request context for each invocation.
- Call exactly one tool service and serialize its approved public response model.
- Map typed application errors to stable MCP errors without exposing connection material.
- Contain FastMCP-specific types so lower layers remain usable in tests and other transports.

Adapters do not resolve Temporal semantics, construct clients, inspect histories, or decide likely
causes.

### Tool services

- Answer one approved operator question.
- Resolve or reauthorize the requested target through the target catalog.
- Orchestrate gateways and, when required, analysis/recommendation components.
- Enforce the tool's approved completeness, refusal, and output-size behavior.
- Assemble the public response from typed facts, findings, and inspection accounting.

Services depend on protocols rather than concrete gateways. They do not import FastMCP or invoke
raw Temporal SDK stubs.

### Optional analysis/recommendation components

- Convert typed Temporal facts into evidence-backed findings, replay impact, or recommendations.
- Remain deterministic and side-effect free.
- Return explicit caveats or refusal results for incomplete and unsupported evidence.
- Never call Temporal, read configuration, construct clients, or format MCP errors.

Analysis is optional because lookup and direct-read tools should not pass through a reasoning
layer merely for structural uniformity.

### Temporal gateways

- Make the Temporal SDK or raw service calls named by an approved tool contract.
- Handle SDK pagination and convert SDK/protobuf values into stable internal fact models.
- Charge calls, pages, and returned events to the request's execution budget.
- Propagate cancellation and apply the remaining request deadline to calls where supported.
- Classify Temporal/transport failures into typed internal errors.

Gateways return facts, not product conclusions. For example, a task-queue gateway can return
current pollers and backlog; it cannot conclude that workers were healthy when a historical
failure occurred.

Prefer capability-focused gateways over one catch-all gateway:

- `VisibilityGateway`
- `WorkflowGateway`
- `HistoryGateway`
- `TaskQueueGateway`
- `ScheduleGateway`
- `BatchGateway`
- `ConnectionGateway`

### Temporal SDK client provider

- Build a `temporalio.client.Client` from a server-side connection descriptor.
- Reuse clients for equivalent descriptors and prevent duplicate concurrent creation.
- Keep credentials, TLS settings, codecs/data converters, and addresses out of tool output.
- Invalidate cached entries when a configured descriptor or credential generation changes.
- Own provider startup/shutdown and release SDK resources when the pinned SDK exposes such a
  lifecycle; otherwise stop new acquisition and discard cached references on shutdown.

The provider does not know workflow IDs, visibility queries, page sizes, tool names, or analysis
rules.

## Cross-cutting concepts

### Request context

Conceptual interface:

```python
@dataclass(frozen=True)
class RequestContext:
    request_id: str
    principal: Principal
    deadline: datetime
    budget: ExecutionBudget
```

For the unauthenticated POC, a factory creates the current demo principal. Later, an HTTP adapter
can construct the same `Principal` from verified Cognito claims without changing tool services or
gateways. Do not store request context in process globals.

### Target catalog

Separate reusable destinations from grants conceptually:

```python
class TargetCatalog(Protocol):
    def list_authorized(self, principal: Principal) -> tuple[ResolvedTarget, ...]: ...
    def match_authorized(
        self, principal: Principal, human_name: str
    ) -> tuple[ResolvedTarget, ...]: ...
    def resolve(self, principal: Principal, human_name: str) -> ResolvedTarget: ...
    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget: ...
```

Discovery and matching inspect only caller-authorized grants, so target names cannot be used as
an existence oracle. `ResolvedTarget` is an internal server-side type containing a stable reference and connection
descriptor. Public target responses expose only contract-approved identifiers. Whether the YAML
format is normalized during this POC is a migration decision; services must not depend on its
storage shape.

### Typed contracts

Keep three distinct families of models:

1. **Public contracts:** versioned MCP inputs, outputs, completeness/inspection metadata, and
   stable error codes owned by the contract workstream.
2. **Internal facts:** loss-bounded representations of Temporal state owned by the corresponding
   gateway workstream.
3. **Analysis results:** findings, evidence references, caveats, and recommendation/refusal values
   owned by the reasoning workstream.

Public models must not directly expose SDK/protobuf objects. Internal facts must preserve enough
provenance (such as event IDs, run IDs, queue type, and observation time) for analysis and audit.
Do not define public fields in this architecture document when the contract review has not approved
their semantics.

### Execution budget

One mutable, concurrency-safe budget belongs to one request context. Gateways and inspection
orchestrators share it; nested components must not create fresh budgets and bypass the caller's
limit.

The **basic budget**, introduced with the foundation, covers every tool:

- absolute request deadline and cancellation;
- maximum Temporal API calls;
- maximum pages fetched;
- maximum history events read;
- maximum serialized output size;
- counters and a typed exhaustion reason.

The **advanced budget**, introduced before population reasoning, adds:

- bounded fan-out concurrency;
- maximum candidates, runs, and histories inspected;
- per-stage allocation and reservation for concurrent work;
- sampling method and seed/provenance;
- per-target rate limiting;
- partial-result policy and inspection reporting.

The contract workstream must decide which exhaustion cases return partial results and which return
errors. Architecture only requires that no result silently appears complete.

Conceptual protocol:

```python
class ExecutionBudget(Protocol):
    def check_deadline(self) -> None: ...
    def reserve_temporal_call(self, operation: str) -> Reservation: ...
    def consume_page(self, operation: str) -> None: ...
    def consume_history_events(self, count: int) -> None: ...
    def snapshot(self) -> InspectionAccounting: ...
```

Reservations matter once calls run concurrently: a check-then-increment implementation can exceed
the limit.

## Dependency and ownership rules

```text
src/ops_mcp/
  mcp/             # adapters and application bootstrap
  services/        # one service per operator capability or cohesive capability group
  analysis/        # deterministic findings/recommendation logic
  gateways/        # Temporal call adapters and SDK-to-fact conversion
  clients/         # Temporal SDK client provider
  runtime/         # request context and execution budgets
  targets/         # target catalog and connection descriptors
  contracts/       # approved public request/response/error models
  facts/           # internal Temporal fact models shared by gateways and analysis
```

Rules:

1. Dependencies point downward through the canonical layers.
2. Analysis consumes typed facts and has no SDK dependency.

3. Only gateways make Temporal operations; only the provider constructs clients.
4. Only adapters import FastMCP.
5. Target authorization occurs in services before every target-bound operation.
6. Public contracts never contain connection descriptors or secrets.
7. Every bounded collection reports contract-approved completeness/accounting metadata.
8. Cross-package cycles are prohibited; shared models move to `contracts`, `facts`, or `runtime`
   rather than being imported from a higher layer.

Tests mirror these boundaries: adapter contract tests, service orchestration tests with gateway
fakes, gateway integration tests against Temporal, and pure analysis tests over a history corpus.

## Auditing and adding a tool

Audit a tool in dependency order: its public sheet in `docs/contracts/`, request and response
models in `src/ops_mcp/contracts/`, MCP signature and description in
`src/ops_mcp/mcp/adapters.py`, service orchestration in `src/ops_mcp/services/`, then every gateway
and deterministic analysis function it calls. Finish at the focused tests and verify that each
claim in `answer_constraints.may_claim` has exact evidence, while every known overclaim appears in
`must_not_claim` or a warning.

To add a tool, first write the operator question, evidence boundary, completion scope, and refusal
rules in a contract sheet. Add strict public models, implement one service method against existing
gateway protocols, and add a gateway capability only when Temporal data is genuinely missing.
Register a thin read-only adapter, wire the service in `ServiceBundle`, document its routing tier,
and add deterministic contract, orchestration, failure, and completeness tests. Do not add
scenario selectors, backend-specific capability variants, or agent-supplied execution controls to
the normal schema.

## Lifecycle boundaries

At application startup:

1. Load and validate configuration and the target catalog.
2. Construct one SDK client provider for the server process.
3. Construct gateways, analysis components, and services by dependency injection.
4. Register adapters against those services.

For each tool invocation:

1. The adapter creates a fresh request context and basic budget.
2. The service resolves or authorizes the target.
3. Gateways acquire a client from the provider and spend the shared budget.
4. The service returns a typed contract response with completeness/accounting where required.

At shutdown, stop accepting requests, allow bounded in-flight calls to cancel or finish, then shut
down the provider. Client cache lifetime is process-scoped; request context and budgets are never
cached. Stdio and streamable HTTP use the same composition root and service lifetimes.

## Compatibility baseline

The POC validates one reproducible environment before claiming broader support:

- Temporal CLI `1.7.0` with its self-hosted dev server `1.31.0`;
- Temporal Python SDK `1.31.0` from `uv.lock`;
- MCP Python SDK `1.29.0` from `uv.lock`;
- the Python version recorded by the test run (currently `3.13.7` locally);
- the namespace and search attributes declared by the test fixture.

Add a machine-readable compatibility manifest and print it in integration-test diagnostics during
migration. The current broad dependency ranges in `pyproject.toml` are not themselves a tested
compatibility claim. Temporal Cloud and other server/SDK versions remain unvalidated until they
are added to the compatibility matrix.

## Product-contract coordination points

The architecture intentionally leaves these questions to the contract lead:

- exact public response envelopes, schema-version policy, and stable error codes;
- whether diagnosis returns findings only or also an operational recommendation;
- how confidence and completeness are represented;
- which budget exhaustion cases yield partial results;
- whether reset calculation and reset recommendation are one tool;
- definition of valid reset points and handling of versioning, side effects, signals, updates,
  child workflows, and continue-as-new;
- whether an advisory command preview and recheck time belong in the reset contract.

Implementers should record unresolved questions rather than embedding answers in gateway or
runtime code.
