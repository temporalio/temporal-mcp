# `get_workflow_history_slice`

- **Operator question:** Show a bounded relevant part of this run's history.
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`; exactly one selector:
  `failures_only`, inclusive `event_id_range`, or `last_n`; optional event-type
  allowlist; opaque continuation token where applicable. Continuation calls retain the
  original request parameters.
- **Output:** ordered normalized events with IDs/types/times and bounded decoded
  attributes, plus inspected and returned ranges.
- **Facts:** history events. **Interpretation:** filtering only.
- **Gateway calls:** paged `GetWorkflowExecutionHistory`; use
  `GetWorkflowExecutionHistoryReverse` for `last_n` when supported.
- **Completeness:** explicitly `complete`, `truncated`, or `budget_exhausted` for
  the requested selector; report events scanned separately from events returned.
- **Pagination:** native page boundaries use Temporal's cursor. If Temporal preserves an
  indivisible batch by returning more events than requested, a checksummed MCP token records the
  native page start, returned prefix length, request scope, and expected next event ID. Resume
  refetches and verifies that page before returning the next bounded slice.
- **Limits/refusals:** hard event/byte/page ceilings; require run ID; reject
  conflicting selectors; never silently drop unknown event types.
- **Errors:** incomplete history, invalid range/token, codec/decode, shared errors.
- **Scenario assertions:** failures-only returns failed activity and closing failure
  with evidence IDs; last 10 does not page from event 1; truncation is visible.
- **Open decisions:** normalized attribute projection and Temporal cursor lifetime.
