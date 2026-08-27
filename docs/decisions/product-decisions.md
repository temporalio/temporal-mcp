# Product decisions

This log records decisions the implementation must not silently make.

## Accepted defaults

### PD-001 — Tool inventory may change during the POC

**Decision:** Phase 1 may rename, split, combine, or remove proposed tools before
external consumers depend on them. A changed operator question requires a renamed
tool.

### PD-002 — Reset output is a preview, never an action

**Decision:** Reset planning returns a structured plan and may include
`command_preview`, explicitly marked `executed: false`. The POC has no reset/write
credential or RPC.

### PD-003 — Reset freshness is advisory

**Decision:** Use `recheck_after`, not `expires_at`. It means recompute before
reliance; it does not invalidate a CLI command. Enforceable expiry is deferred
with write tooling.

### PD-004 — Customer policy is outside this POC

**Decision:** Tools return portable Temporal facts and evidence-backed semantic
findings. Customer incident policy, dependency-health interpretation, and business
side-effect safety belong to runbooks or later configured policy.

### PD-005 — Compatibility target is narrow and explicit

**Decision:** Success means one pinned self-hosted Temporal dev-server version and
one pinned Python SDK version. Temporal Cloud compatibility is unproven.

### PD-006 — Authentication is deferred, its seam is not

**Decision:** The demo principal remains request-scoped behind an identity
interface. Cognito and production authorization are not implemented in this POC.

### PD-007 — Deterministic tests define correctness

**Decision:** Contract, gateway, golden-history, scenario, budget, and truncation
tests are normative. Codex CLI is the primary agent demo; agent improvisation is
not the sole completion criterion.

### PD-008 — Completeness and confidence are categorical

**Decision:** Collection completion is `complete | truncated | sampled |
budget_exhausted | unknown`. Finding confidence is `high | medium | low |
unknown`; no numeric confidence.

### PD-009 — Workflow results do not wait by default

**Decision:** `get_workflow_result` returns running state immediately. Waiting must
be explicit and deadline-bounded.

### PD-010 — Population tools require a bounded population

**Decision:** Population reasoning requires a visibility query/time window and
explicit budgets. Sampling method and inspected size are always reported.

### PD-011 — Diagnosis does not infer recovery

**Decision:** Retry exhaustion and current pollers are facts; retry worthiness and
dependency recovery remain `unknown` without separate evidence. Current health is
not historical health.

### PD-012 — Reset recommendation may be absent

**Decision:** A planner may return valid points and replay facts while declining a
recommendation. Complete history and supported topology are prerequisites.

### PD-013 — Reset event semantics are pinned for the POC baseline

**Decision:** For CLI 1.7/server 1.31/SDK 1.31, supported reset boundaries are
`WorkflowTaskCompleted`, `WorkflowTaskFailed`, and `WorkflowTaskTimedOut`; the
CLI receives that finish event's exact ID unchanged. This establishes validity,
not operational safety. See [reset-event-semantics.md](reset-event-semantics.md).

### PD-019 — Temporal owns Visibility query capabilities

**Decision:** The MCP server exposes registered search-attribute names, sources,
and indexed types, but it does not maintain a per-backend grammar/operator matrix
or emulate unsupported ordering. The selected Temporal target validates each raw
Visibility query. A rejected query returns a stable `invalid_visibility_query`
code with bounded, sanitized rejection feedback so an agent can revise it.

## Decisions required before implementation

### PD-014 — Codec probe protocol

Define the configured codec health/decode probe request, acceptable response, and
trusted endpoint policy before implementing `check_connection` codec validation.

### PD-015 — Schedule list pagination under local filters

Decide whether `page_size` counts matching schedules returned or raw server rows
scanned. The contract currently requires both to be observable either way.

### PD-016 — Query allowlist configuration

Define per-target approved no-argument query names and whether results require a
declared output sensitivity/size policy.

### PD-017 — Diagnostic retry assessment

Confirm whether `retry_assessment` remains a four-state output or is removed in
favor of findings. Current recommendation: retain it, with `unknown` as the normal
portable answer absent external recovery and idempotency evidence.

### PD-018 — Population sampling defaults

Choose a deterministic default sampling method, reported seed, maximum fan-out,
and minimum useful sample size after measuring the pinned server.
