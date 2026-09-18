# Temporal Ops MCP prototype

A read-only MCP server that answers operator questions by joining and reducing Temporal API
responses. The default server implements the layered routing, building-block, evidence-composite,
and operator-intent surface described in [`docs/tool-surface-v2.md`](docs/tool-surface-v2.md).

Authentication is intentionally deferred until after the unauthenticated demo.
The target registry already scopes every target to caller groups so a Cognito
claim adapter can replace the development principal without changing tool APIs.

## Quick start

Requirements: Python 3.11+, `uv`, and the Temporal CLI.

```bash
uv sync
temporal server start-dev --search-attribute OrderId=Keyword
```

In a second terminal, start the independent demo worker:

```bash
uv run python -m ops_mcp.demo.worker
```

In a third terminal, seed the failed order and twelve recovered orders:

```bash
uv run python -m ops_mcp.demo.seed
```

Then start the MCP server using stdio (the simplest local demo transport):

```bash
uv run ops-mcp
```

`ops-mcp` launches the generalized layered surface.

The preferred read-only surface is layered around routing, building blocks, evidence composites,
and operator intent. Its key additions are `search_workflow_runs`,
`list_approved_workflow_queries`, `inspect_workflow_execution`,
`inspect_workflow_task_failures`, `inspect_activity_queue_dependencies`,
`analyze_incident_impact`, and `assess_workflow_recovery`.
See [`docs/tool-surface-v2.md`](docs/tool-surface-v2.md) for routing and continuation behavior. The
default transport is stdio. Workflow queries are denied by default until the composition root
receives an explicit per-target query allowlist.

### Session 2A: diagnose one long-running workflow

Session 2 uses separate test infrastructure. The primary acceptance case asks whether one exact,
long-running workflow can currently make progress. The larger seeded population exists for the
separate Session 2B blast-radius exercise; it must not be used to infer that every workflow is
blocked merely because it uses a workflow task queue named `ledger`.

The fixture creates 1,830 running workflows across four workflow types and schedules 4,200
`FetchLedgerBatch` activity tasks on the `ledger` activity task queue. Start the workflow-only
worker first:

```bash
uv run python -m ops_mcp.demo_stuck.worker
```

Then seed the fixture once:

```bash
uv run python -m ops_mcp.demo_stuck.seed
```

No activity worker is started, so workflow queries remain available while the activity queue has
zero pollers. The primary workflow ID is `session2-payment-reconciliation-main`. The Python SDK
automatically registers the built-in no-argument `__stack_trace` query; the fixture does not define
a custom `blocked_on` query. Set
`OPS_MCP_QUERY_APPROVALS_JSON='{"orders-prod":["__stack_trace"]}'` in the environment that launches
the generalized MCP server.

For Session 2A, describe the exact run, inspect its pending activities, query `__stack_trace`, and
describe the `ledger` **activity** task queue. A valid conclusion is that this run cannot dispatch
its pending activities at the observation time. The current queue snapshot does not prove how long
pollers have been absent.

### Session 2B: assess blast radius

Treat visibility results only as candidates. `TaskQueue = 'ledger'` is workflow-task-queue
membership; it is not evidence that a run has a pending activity on an activity task queue with the
same name. Confirm an affected run only by inspecting that run and establishing a pending activity
on `ledger`. Report confirmed, unknown, failed-to-inspect, and completeness counts. If inspection
is not exhaustive, report a lower bound or sample—not "all 1,830 workflows are blocked."

The 4,200 task-queue backlog is an approximate task count, not a unique-workflow count. Stop the
fixture worker when testing is complete. Restarting an in-memory Temporal dev server removes the
seeded workflows.

### Sessions 3 and 4: population evidence boundaries

Each population call collects a non-overlapping bounded candidate frame before selecting a
deterministic hash sample. When another native frame remains, the signed continuation retains only
its next Temporal Visibility cursor, bounded scalar progress, and fixed-size group summaries.
It does not retain workflow/run identity ledgers, so token size stays bounded as later calls advance
through a large query population without timestamp partitioning or rereading prior frames.
Candidate collection, selected runs, successful history reads, failures, and queue reads remain
separate in the response. The server never promotes a bounded sample into an exact population
count.

`search_workflow_runs` plus population aggregation expose custom search attributes only when their
registered names are explicitly requested. Team ownership, money/domain classification, deploy
time, Build IDs, normal-duration policy, and historical queue health are not inferred. They must
come from registered attributes, operator input, deployment/catalog integrations, or metrics.

### Session 3 fixture: a real unsafe replay boundary

The live fixture is intentionally smaller than production but preserves the customer question:
230 workflows exist before the deploy, 190 receive work on the post-deploy worker, and 43 of those
had taken the shipping branch whose activity command was unsafely removed. The affected IDs are
not indexed; the only honest way to find them is bounded history inspection.

Run these in order:

```bash
uv run python -m ops_mcp.demo_nondeterminism.worker_v1
uv run python -m ops_mcp.demo_nondeterminism.seed
# Stop the v1 worker, then start the v2 worker in that terminal.
uv run python -m ops_mcp.demo_nondeterminism.worker_v2
uv run python -m ops_mcp.demo_nondeterminism.expose
```

The expose command prints a ready-to-paste customer question with the actual deploy timestamp.
The acceptance answer must separate 230 pre-deploy candidates, successful v2 processing,
inspected histories, and confirmed non-determinism failures. Live validation exposed an important
correction to the original scenario: `BuildIds=unversioned:fulfillment-v2` contains the 147 runs
whose v2 workflow task succeeded, but excludes the 43 v2 workflow tasks that failed replay. A
failed run retains `fulfillment-v2` on its `WorkflowTaskStarted` history event, which the
population finding now exposes as `worker_build_id`. Therefore `BuildIds` is not an honest exposed
set for this failure mode. A complete-frame sample over the 230 candidates may support an
estimate; an exact answer requires exhaustive history inspection. The precise source-code cause
is fixture ground truth; an agent may state it only when Temporal failure evidence contains the
divergence.

### Session 4 fixture: ownership, money domain, and incident recovery

Start the workflow-only worker, then seed while no activity worker exists:

```bash
uv run python -m ops_mcp.demo_incident.workflow_worker
uv run python -m ops_mcp.demo_incident.seed
```

The seed command registers the `Team` and `Domain` custom search attributes, creates 160 workflows
across six teams, and prints an absolute incident window plus a ready-to-paste customer question.
Ninety workflows carry `Domain=payments`; that attribute—not workflow naming—is the money-impact
evidence. After recording the incident end, model recovery in another terminal:

```bash
uv run python -m ops_mcp.demo_incident.activity_worker
```

Recovery workers now appear healthy while histories retain the schedule-to-start delay and retry
evidence. The acceptance answer must provide the per-team/domain breakdown, keep start-window
candidates separate from confirmed impact, and say that current pollers do not prove historical
poller health. The printed five-second normal-duration policy is deliberately external input.

The HTTP transport is not authenticated yet and is disabled by default. For a loopback-only demo,
set both `OPS_MCP_TRANSPORT=streamable-http` and `OPS_MCP_ALLOW_INSECURE_HTTP=1` to expose `/mcp`.
Never expose this mode to an untrusted network. HTTP mode also requires a stable, secret
`OPS_MCP_CONTINUATION_SECRET` of at least 32 bytes so continuations work across restarts and
multiple processes.

## Configuration

In a source checkout, `config/targets.example.yaml` demonstrates group-to-target mappings. A wheel
does not install that development fixture: create a target file and set `OPS_MCP_TARGET_CONFIG` to
its absolute path. A minimal local configuration is:

```yaml
groups:
  demo-support:
    targets:
      orders-local:
        aliases: [orders local]
        account: local
        cluster: temporal-dev
        namespace: default
        address: 127.0.0.1:7233
        tls: false
```

Select the unauthenticated demo group with `OPS_MCP_DEMO_GROUP`. Connection credentials stay in
server config: set `api_key_env` on a target to the *name* of an environment variable holding the
key. Never put the key itself in YAML.

The local defaults are:

```bash
export OPS_MCP_TARGET_CONFIG="$PWD/config/targets.example.yaml"
export OPS_MCP_DEMO_GROUP=demo-support
export OPS_MCP_QUERY_APPROVALS_JSON='{"orders-prod":["__stack_trace","progress"]}'
```

`OPS_MCP_QUERY_APPROVALS_JSON` is a server-side mapping from each `target_ref` to exact,
no-argument workflow query names. It must be a JSON object whose values are arrays of non-empty
strings. Omit it (or set `{}`) to deny every workflow query. Invalid values stop server startup.

## Safety boundary

All generalized tools are read-only. `plan_workflow_reset` analyzes history and emits a
reviewable CLI command; it does not reset a workflow. Plans are pinned to a run and record the
observed history length. `recheck_after` is advisory: recompute the plan after that time or whenever
the run's history may have changed; the server does not maintain or expire plan state.
