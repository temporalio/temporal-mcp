# `get_workflow_history_slice`

- **Operator question:** Show a bounded relevant part of this run's history.
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`; exactly one selector:
  `failures_only`, inclusive `event_id_range`, or `last_n`; optional event-type
  allowlist; opaque page token where applicable.
- **Output:** ordered normalized events with IDs/types/times and bounded decoded
  attributes, plus inspected and returned ranges.
- **Facts:** history events. **Interpretation:** filtering only.
- **Gateway calls:** paged `GetWorkflowExecutionHistory`; use
  `GetWorkflowExecutionHistoryReverse` for `last_n` when supported.
- **Completeness:** explicitly `complete`, `truncated`, or `budget_exhausted` for
  the requested selector; report events scanned separately from events returned.
- **Limits/refusals:** hard event/byte/page ceilings; require run ID; reject
  conflicting selectors; never silently drop unknown event types.
- **Errors:** incomplete history, invalid range/token, codec/decode, shared errors.
- **Scenario assertions:** failures-only returns failed activity and closing failure
  with evidence IDs; last 10 does not page from event 1; truncation is visible.
- **Open decisions:** normalized attribute projection and pagination token lifetime.
