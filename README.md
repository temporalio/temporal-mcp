# Temporal Ops MCP

A read-only MCP server for investigating Temporal workflows. It turns bounded Temporal API reads
into evidence an operator—or an agent acting for one—can use without giving the MCP server mutation
authority.

This is a prototype. Stdio is the default transport, authentication is not yet implemented, and
the optional HTTP transport is for loopback demos only.

## What to know

- **Targets are server configuration.** Operators refer to an authorized name such as
  `orders prod`; addresses, TLS settings, and credentials never enter tool arguments or responses.
- **Tools have different evidence scopes.** Direct reads return Temporal facts. Composite tools
  join several facts to answer one operational question. A reset plan is analysis, not execution.
- **Claims are narrower than data retrieval.** Visibility results are candidates until exact-run
  evidence confirms impact. Current queue health does not establish historical health, and an
  approximate backlog is not a workflow count.
- **Population work is bounded and resumable.** Large investigations report what was considered,
  selected, inspected, confirmed, and missed. Signed continuation tokens advance through
  non-overlapping Visibility frames without embedding workflow identities or raw queries.
- **Completeness is part of every conclusion.** Partial, sampled, truncated, and failed reads stay
  visible; the server does not turn a bounded sample into an exact population claim.

For the concise conceptual and code map, start with the
[reviewer guide](docs/reviewer-guide.md). The fuller design is in
[architecture](docs/architecture.md), and exact routing and continuation behavior is in the
[tool-surface guide](docs/tool-surface-v2.md).

## Architecture

```text
MCP adapter
    -> tool service
        -> optional deterministic analysis
            -> capability-focused Temporal gateway
                -> shared Temporal client provider
```

Adapters validate and serialize. Services own the operator question, authorization, budgets, and
completeness. Analysis turns typed facts into evidence-backed findings without making network
calls. In the server request path, gateways are the only components that call Temporal, and the
client provider is the only component that creates or caches SDK clients. Standalone demo and
acceptance fixtures have their own Temporal lifecycle.

## Tool model

| Layer | Purpose | Examples |
| --- | --- | --- |
| Routing | Resolve an authorized target and verify connectivity | `resolve_target`, `check_connection` |
| Direct reads | Read one focused Temporal fact | `search_workflow_runs`, `describe_workflow`, `get_workflow_history_slice`, `describe_task_queue` |
| Evidence composites | Join facts for an exact run or bounded population | `inspect_workflow_execution`, `inspect_workflow_task_failures`, `inspect_activity_queue_dependencies` |
| Operator intent | Answer an incident, recovery, or planning question | `analyze_incident_impact`, `assess_workflow_recovery`, `plan_workflow_reset` |

Resolve a target once and reuse its opaque `target_ref`. Prefer a matching composite over manually
rebuilding it from direct reads. Use population tools only for an explicit population question;
do not broaden an exact-run investigation into a blast-radius scan.

Workflow queries are denied by default. `list_approved_workflow_queries` exposes the configured
no-argument query names for a target, while `query_workflow` executes only an approved name.

## Quick start

Requirements: Python 3.11+, `uv`, and the Temporal CLI.

```bash
uv sync
temporal server start-dev --search-attribute OrderId=Keyword
```

In a second terminal, run the Session 1 worker:

```bash
uv run python -m ops_mcp.demo.worker
```

In a third terminal, seed one failed order and twelve later successful orders:

```bash
uv run python -m ops_mcp.demo.seed
```

Then start the MCP server from the repository checkout:

```bash
export OPS_MCP_TARGET_CONFIG="$PWD/config/targets.example.yaml"
export OPS_MCP_DEMO_GROUP=demo-support
export OPS_MCP_QUERY_APPROVALS_JSON='{"orders-prod":["__stack_trace","progress"]}'
uv run ops-mcp
```

The checkout example resolves `orders prod` to the local Temporal dev server. Outside a source
checkout, `OPS_MCP_TARGET_CONFIG` must point to an explicit configuration file.

## Configuration and safety

Target configuration maps caller groups to named Temporal destinations. Connection credentials
remain server-side: `api_key_env` names an environment variable containing a key; the key itself
must not appear in YAML.

All tools are read-only. `plan_workflow_reset` may return a reviewable Temporal CLI command marked
`executed: false`; it never runs that command. Plans are pinned to an exact run and observed history
length. Their `recheck_after` value is advisory, not server-enforced expiry.

The HTTP transport is unauthenticated and disabled by default. A loopback-only demo requires both
`OPS_MCP_TRANSPORT=streamable-http` and `OPS_MCP_ALLOW_INSECURE_HTTP=1`. It also requires an
`OPS_MCP_CONTINUATION_SECRET` of at least 32 bytes so continuation tokens remain valid across
processes and restarts. Do not expose this mode to an untrusted network.

## Validation and deeper documentation

- [Reviewer guide](docs/reviewer-guide.md): concepts, code map, and review invariants.
- [Architecture](docs/architecture.md): layer responsibilities and dependency rules.
- [Tool surface](docs/tool-surface-v2.md): tool tiers, evidence rules, and continuation semantics.
- [Contract index](docs/contracts/README.md): public request and response behavior.
- [Scenario matrix](docs/scenario-matrix.md): capability and evidence coverage.
- [Validation plan](docs/validation-plan.md): deterministic and agent-driven acceptance criteria.
- [Live test infrastructure](docs/test-infrastructure.md): isolated Sessions 1–4 and noisy-mode runs.

Run the normal test suite with:

```bash
uv run pytest -m "not live_temporal"
```
