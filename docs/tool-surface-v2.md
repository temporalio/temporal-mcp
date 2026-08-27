# Layered MCP tool surface

The preferred tool surface makes responsibility visible in each name:

- routing: `resolve_target`, `check_connection`;
- building blocks: `search_workflow_runs`, `count_workflows`, `get_*`, `describe_*`,
  `query_workflow`, and `list_*`;
- evidence composites: `inspect_workflow_execution`, `inspect_workflow_task_failures`, and
  `inspect_activity_queue_dependencies`;
- operator intent: `analyze_incident_impact`, `assess_workflow_recovery`, and
  `plan_workflow_reset`.

Resolve a target once and reuse its `target_ref`. Prefer an `inspect_*` tool when its documented
evidence scope matches the question. Building blocks are for novel questions, optional enrichment,
or evidence a composite explicitly reports as unavailable. `analyze_incident_impact` requires an
absolute incident window. `assess_workflow_recovery` inspects exact outcomes of one activity type
in comparable runs; a success supports recovery but is not a direct dependency-health check.
`plan_workflow_reset` is used only for an explicit reset-plan request after recovery evidence is
available and never executes its preview.

Routing is tiered. An exact-run question starts and normally ends with
`inspect_workflow_execution`. Population composites are used only for explicit blast-radius or
incident questions. Building blocks are the escape hatch when no composite matches or a composite
reports missing evidence.

| Tier | Tools | Routing rule |
|---|---|---|
| Operator-intent composites | `inspect_workflow_execution`, `inspect_workflow_task_failures`, `analyze_incident_impact`, `assess_workflow_recovery` | Start here when the operator question matches the intent. |
| Focused evidence composites | `inspect_activity_queue_dependencies` | Use for a specific missing dependency/dispatch evidence join. |
| Building blocks | `search_workflow_runs`, `get_workflow_history_slice`, `describe_workflow`, `describe_task_queue`, `query_workflow`, `count_workflows` | Use for novel investigations or evidence explicitly unavailable from a composite. |
| Planning | `plan_workflow_reset` | Use only after exact-run and recovery evidence are established. |

Do not abandon a suitable composite merely because building blocks could reproduce it. Do not
broaden an exact-run question into population analysis without operator intent or a stated
evidence requirement.

## Migration aliases

The following aliases remain for one migration cycle:

| Preferred | Deprecated compatibility alias |
|---|---|
| `search_workflow_runs` | `filter_workflows` |
| `inspect_workflow_execution` | `diagnose_workflow` |
| `inspect_workflow_task_failures` | `find_workflow_task_failures` |
| `inspect_activity_queue_dependencies` | `find_stuck_workflows` |

`query_workflow` now requires an approved query name at the MCP boundary. Use
`list_approved_workflow_queries` to read configured names and meanings without making a Temporal
RPC. Configuration does not prove that a particular worker supports a query.

## Defaults and resumability

Preferred population tools own sampling seeds, concurrency, page limits, candidate limits, and
history limits. These controls are intentionally absent from their normal MCP schemas; deprecated
expert aliases retain explicit controls during migration. Time fields use `started_at` and
`ended_at`; omitted population windows default to the server's broad bounded window.

Population composites return a signed opaque `continuation_token` while runs remain in the current
frame or Temporal has another Visibility page. The token binds the target, normalized scope,
effective time window, sampling seed, schema version, native candidate-frame cursor, and
inspected/confirmed identity ledgers and cumulative group totals. Tampered tokens or reuse with
changed scope are rejected. Each response separates `page` from `cumulative` progress and returns
a compact `next_call`. Only a terminal response may describe its stated scope as complete.
Continuation advances through arbitrarily large query populations without asking the caller to
invent timestamp partitions; each response still reports whether its current candidate collection
was complete.

All preferred paginated tools use only `continuation_token`. Compatibility aliases retain their
older pagination fields for one migration cycle.

## Evidence rules

Composite responses explicitly list evidence collected, evidence unavailable, supported
conclusions, unsupported conclusions, inspection completeness, warnings, and any continuation.
Current task-queue snapshots are never historical health. Workflow-task queue membership is never
activity dependency evidence. Activity dependencies are confirmed from a pending activity's queue
or activity scheduling history. Team and monetary-domain grouping uses registered Search Attributes,
not workflow names.

For incident analysis, `candidate_query` defines the operational population,
`affected_activity_task_queues` defines the impact criterion, and `business_grouping` only groups
confirmed results by raw registered values. Explicitly excluding a discovered queue produces
`completion_scope=explicit_queue_subset` and can never produce a complete incident census.
