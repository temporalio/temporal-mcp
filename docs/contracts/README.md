# Tool contract registry

Status: proposed for the generalizable read-only POC.

These sheets define product behavior; code does not. The POC targets one pinned
self-hosted Temporal dev-server and Python SDK version. Temporal Cloud,
authentication, write operations, and customer-specific runbook policy are
deferred.

## Shared vocabulary

All successful results conceptually include:

```text
schema_version: "1"
target_ref: opaque configured target ID
answer: tool-specific typed value
inspection:
  completion: complete | truncated | sampled | budget_exhausted | unknown
  temporal_calls: integer
  pages_read: integer
  events_read: integer | null
  runs_inspected: integer | null
  limit: relevant applied limit | null
warnings: list[string]
```

`complete` means complete only for the stated query and requested page/range.
It never means that an operator fetched every page unless the response says so.

Reasoning tools return zero or more findings:

```text
code: stable machine-readable identifier
summary: factually bounded statement
confidence: high | medium | low | unknown
evidence: list[EvidenceRef]
counterevidence: list[EvidenceRef]
caveats: list[string]
```

Confidence means:

- `high`: directly established by Temporal state or history.
- `medium`: supported inference missing relevant historical context.
- `low`: plausible hypothesis requiring external evidence.
- `unknown`: evidence is insufficient.

Tools must never turn current worker health into historical worker health, a
retry state into dependency recovery, or a sample into a population claim.
They must also never join visibility `TaskQueue` (the workflow task queue) to an
activity task queue merely because the names are equal. An affected-run claim needs
pending-activity evidence for that exact run.

Preferred `inspect_*`, `analyze_*`, and `plan_*` contracts additionally expose
`evidence_collected`, `evidence_unavailable`, `supported_conclusions`,
`unsupported_conclusions`, `continuation_token`, and a standardized inspection core:

```text
completion: complete | partial | sampled | truncated | budget_exhausted
candidate_count: integer
runs_inspected: integer
histories_inspected: integer
inspection_failures: integer
```

Preferred paginated tools accept and return only `continuation_token`; deprecated aliases may
retain legacy token names for one migration cycle. See the
[layered tool surface](../tool-surface-v2.md) for routing and migration rules.

## Schema v1 compatibility

The Session 2 evidence gaps should be closed without reinterpreting existing v1
fields:

- `describe_workflow.answer.task_queue` continues to mean the workflow task queue.
- Pending-activity task queue, heartbeat time/details, attempt, and related timing
  facts may be added as optional fields beneath each pending activity.
- Missing optional evidence means unavailable/not reported, not a negative fact.
- Approving the SDK built-in `__stack_trace` query changes server policy, not the
  `query_workflow` response schema.
- Population counts already emitted by v1 keep their documented meanings. They
  must not be relabeled "confirmed affected" unless the contract supplies exact-run
  confirmation and inspection-completeness fields.

Removing a field, making new evidence mandatory, changing `task_queue` semantics,
or overloading an existing count requires schema v2 or a renamed capability.

## Shared errors

Errors are typed as `{code, message, retryable, details?}`. Baseline codes:

- `invalid_argument`
- `target_not_found`
- `target_not_authorized`
- `temporal_unreachable`
- `temporal_authentication_failed`
- `namespace_not_found`
- `invalid_visibility_query`
- `workflow_not_found`
- `run_not_found`
- `unsupported_capability`
- `codec_unreachable`
- `payload_decode_failed`
- `query_not_approved`
- `query_failed`
- `deadline_exceeded`
- `budget_exhausted`
- `result_too_large`
- `incomplete_history`
- `unsafe_to_recommend`
- `internal_error`

Unknown SDK/RPC errors map to `internal_error` without exposing credentials or
network configuration.

For `invalid_visibility_query`, `details.rejection_reason` contains a bounded,
sanitized version of Temporal's rejection. It is intended to help an agent revise
and retry a query; Temporal remains authoritative for the selected target's query
grammar, operators, grouping, and ordering support.

## Registry

| Capability | Class | Contract | Status |
|---|---|---|---|
| `check_connection` | setup | [sheet](check_connection.md) | proposed |
| `resolve_target` | setup | [sheet](resolve_target.md) | proposed |
| `get_visibility_schema` | direct read | [sheet](get_visibility_schema.md) | proposed |
| `search_workflow_runs` | paged read | [compatibility sheet](filter_workflows.md) | preferred |
| `filter_workflows` | paged read | [sheet](filter_workflows.md) | proposed |
| `count_workflows` | aggregate read | [sheet](count_workflows.md) | proposed |
| `describe_workflow` | direct read | [sheet](describe_workflow.md) | proposed |
| `get_workflow_history_slice` | bounded read | [sheet](get_workflow_history_slice.md) | proposed |
| `get_workflow_result` | bounded read | [sheet](get_workflow_result.md) | proposed |
| `describe_task_queue` | direct read | [sheet](describe_task_queue.md) | proposed |
| `list_schedules` | paged read | [sheet](list_schedules.md) | proposed |
| `describe_schedule` | direct read | [sheet](describe_schedule.md) | proposed |
| `query_workflow` | direct read | [sheet](query_workflow.md) | proposed |
| `list_approved_workflow_queries` | discovery | [migration guide](../tool-surface-v2.md) | preferred |
| `list_batch_operations` | bounded join | [sheet](list_batch_operations.md) | proposed |
| `diagnose_workflow` | reasoning | [sheet](diagnose_workflow.md) | revise existing |
| `inspect_workflow_execution` | evidence composite | [migration guide](../tool-surface-v2.md) | preferred |
| `inspect_workflow_task_failures` | evidence composite | [sheet](find_workflow_task_failures.md) | preferred |
| `inspect_activity_queue_dependencies` | evidence composite | [sheet](find_stuck_workflows.md) | preferred |
| `find_workflow_task_failures` | population reasoning | [sheet](find_workflow_task_failures.md) | proposed |
| `find_stuck_workflows` | population reasoning | [sheet](find_stuck_workflows.md) | proposed |
| `plan_workflow_reset` | recommendation | [sheet](plan_workflow_reset.md) | revise existing |

## Contract change rule

Adding optional fields is compatible. Removing fields, changing meaning,
changing default blocking/completeness behavior, or weakening a refusal requires
a new schema version; changing the operator question requires a renamed tool.
