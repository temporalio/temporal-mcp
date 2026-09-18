# Reviewer guide

This guide is the shortest path to understanding Temporal Ops MCP. It describes the concepts and
boundaries that should remain true as the implementation evolves; it is not a changelog or a list
of branch-specific edits.

## The product in one sentence

Temporal Ops MCP is a read-only evidence layer between an MCP consumer and Temporal: it resolves an
authorized target, performs bounded reads, and returns facts or evidence-backed conclusions with
explicit completeness.

It is not a Temporal proxy, an autonomous remediation system, or a source of historical facts that
Temporal does not retain.

## The concepts that matter

### Targets are capabilities, not connection strings

A caller selects a human name such as `orders prod`. The target catalog resolves that name only
within the caller's grants and returns an opaque `target_ref`. Every target-bound service
reauthorizes that reference. Addresses, TLS configuration, credentials, and credential-generation
state stay on the server.

This keeps connection material out of MCP schemas and prevents target discovery from becoming an
existence oracle.

### Direct reads and conclusions are different products

Direct-read tools expose focused Temporal facts: a Visibility page, a workflow description, a
history slice, a task-queue snapshot, or a workflow query result. Composite tools answer a larger
operator question by joining those facts under a deterministic playbook.

The distinction matters because retrieval does not automatically justify a conclusion. For
example:

- a Visibility match is a candidate, not necessarily an affected workflow;
- workflow-task-queue membership does not prove dependency on an activity queue with the same name;
- a current zero-poller snapshot does not establish when pollers disappeared;
- `BuildIds` records successful workflow-task processing and can omit a worker build whose replay
  failed before completion;
- elapsed time indicates abnormal duration only when compared with an external policy.

### Exact-run and population investigations have different costs

An exact-run investigation should normally start and end with `inspect_workflow_execution`. It may
combine workflow state, bounded history, activity and workflow queue snapshots, and an approved
diagnostic query.

A population investigation begins with a Visibility candidate set, selects a deterministic bounded
frame, and fans out only within request limits. It must keep these quantities distinct:

```text
confirmed <= successfully inspected <= attempted <= selected <= candidates
```

An exact count requires exhaustive evidence for the stated scope. Otherwise the result is a lower
bound, sample, estimate, partial result, or unknown—not a census.

### Continuations resume bounded work

Pagination and composite continuation use an opaque `continuation_token`. Population tokens are
signed and scoped to the target, tool, query/window, and relevant request shape. They carry the next
native Temporal cursor plus bounded cumulative accounting; they do not carry raw queries or an
ever-growing ledger of workflow IDs.

A continuation advances through non-overlapping Visibility frames. Completeness can only stay the
same or weaken as later frames add sampling, truncation, failures, or group overflow. Invalid,
tampered, stale, or cross-scope tokens fail safely.

### Evidence has a time boundary

Every operational claim should say whether it describes current state, observed history, a
server-reported index, or external policy. In particular:

- current task-queue pollers and backlog are point-in-time evidence;
- history can preserve past retries, failures, and schedule-to-start delay;
- registered Search Attributes may support ownership or domain grouping;
- deployment times, normal-duration policy, and historical queue health require operator input or
  another integration when Temporal does not provide them.

The server should expose unavailable evidence rather than fill the gap with an inference.

### Reset planning is still read-only

`plan_workflow_reset` analyzes a pinned run and observed history. It can describe valid reset points,
preserved and replayed work, reapplied signals or updates, and a non-executed command preview. It
does not prove external side-effect safety and never performs a reset. `recheck_after` is an
advisory freshness signal; no plan state or expiry is enforced by the server.

## How the code is divided

```text
src/ops_mcp/
  mcp/          transport adapters and composition root
  services/     authorization, orchestration, completeness, public responses
  analysis/     deterministic interpretation of typed facts
  gateways/     Temporal calls and SDK/protobuf conversion
  clients/      SDK client acquisition, reuse, invalidation, and shutdown
  runtime/      request context, budgets, and continuations
  targets/      configured destinations and caller grants
  contracts/    public MCP request and response models
  facts/        internal Temporal fact models
  demo*/        standalone workflow fixtures, workers, and seeders
  acceptance/   isolated live-fixture process orchestration
```

The dependency direction is:

```text
MCP adapters -> services -> optional analysis -> gateways -> client provider
```

Within the server request path, only adapters know FastMCP, only gateways call Temporal, and only
the provider constructs clients. Analysis is deterministic and side-effect free. Services own
authorization, orchestration, and the meaning of completeness. Standalone demos and acceptance
fixtures are deliberately outside that request path and manage their own Temporal processes.

## A practical review path

1. Start at `src/ops_mcp/mcp/app.py` for consumer-visible routing instructions and tool
   registration.
2. Read `src/ops_mcp/mcp/composition.py` to see process-lifetime dependencies.
3. Follow one adapter in `src/ops_mcp/mcp/adapters.py` into its service.
4. Confirm the service reauthorizes the target, shares one request budget, and reports incomplete
   evidence honestly.
5. Follow gateway calls to confirm SDK values become typed facts before analysis or serialization.
6. For population tools, inspect `src/ops_mcp/services/population.py` together with
   `src/ops_mcp/runtime/continuation.py`.
7. Finish with the focused contract, service, gateway, continuation, and scenario tests.

## Review invariants

A change deserves extra scrutiny if it does any of the following:

- adds Temporal API operations to the server request path outside `gateways/`;
- constructs clients outside `clients/`;
- exposes connection details, credentials, or raw server configuration;
- treats a candidate or sample as a confirmed population;
- converts a missing read into a guessed fact;
- backdates current queue state;
- allows agent-supplied limits to bypass server budgets;
- creates a continuation that can grow with population identity count;
- changes a public contract without updating its contract sheet and scenario assertions;
- makes reset planning capable of mutation.

## Where to go next

- [`architecture.md`](architecture.md) defines layer responsibilities and lifecycle boundaries.
- [`tool-surface-v2.md`](tool-surface-v2.md) defines routing tiers and continuation behavior.
- [`contracts/README.md`](contracts/README.md) indexes the public tool contracts.
- [`scenario-matrix.md`](scenario-matrix.md) maps capabilities to evidence and failure cases.
- [`validation-plan.md`](validation-plan.md) defines deterministic and model-dependent acceptance.
- [`test-infrastructure.md`](test-infrastructure.md) explains the isolated Temporal fixtures.
