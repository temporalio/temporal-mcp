# `count_workflows`

- **Operator question:** How many runs match, optionally grouped by a supported
  visibility dimension?
- **Inputs:** `target_ref`, `query: str`, `group_by?: status`. The pinned backend accepts
  server-side grouping by `ExecutionStatus`; workflow-type grouping is not advertised because
  this backend rejects it. The tool does not maintain a backend capability matrix or emulate
  rejected raw Visibility aggregates client-side.
- **Output:** server-reported approximate aggregate `count`, normalized group
  keys/counts, effective query, observation time, and `precision: approximate`.
- **Facts:** visibility aggregate. **Interpretation:** none; counts do not prove a
  dependency cause or recovery.
- **Gateway calls:** `CountWorkflowExecutions` with supported grouping expression.
- **Completeness:** `complete` means the single server aggregate call completed;
  it does not upgrade the pinned SDK's approximate count to exact. Otherwise
  `unknown`; never sampled.
- **Limits/refusals:** one approved grouping dimension in POC; reject unsupported
  groupings rather than emulate with list-and-count.
- **Errors:** `invalid_visibility_query`, `invalid_argument`, shared target and
  connection errors. `invalid_visibility_query` includes a bounded, sanitized
  Temporal rejection reason so the caller can revise the query.
- **Scenario assertions:** the pinned fixture reports twelve completed
  `OrderWorkflow` runs in the defined time window, labels the value approximate,
  and contains no workflow list.
- **Optimization:** multiple statuses can be counted in one call with a raw query such as
  `ExecutionStatus IN ('Failed', 'TimedOut')` and `group_by=status`. Visibility is eventually
  consistent, so compare list/count snapshots using their observation times; do not treat the
  approximate count as exact.
- **Open decisions:** whether arbitrary server-supported group-bys enter v1.
- **Query capability boundary:** Temporal validates the effective visibility
  query on the selected target. This MCP server does not maintain a per-backend
  grammar or operator capability matrix.
