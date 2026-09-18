# `inspect_workflow_execution`

This file retains its legacy filename as a historical documentation link; the retired pre-release
alias is not present in production code.

- **Operator question:** What Temporal evidence explains this run's current or
  terminal condition, and what remains unknown?
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`; optional bounded
  `history_event_limit` and optional positive caller-supplied
  `expected_duration_seconds`.
- **Output:** workflow facts, pending/wait-state facts, failure and retry findings, task queues
  inspected, evidence availability, supported/unsupported conclusions, and current dispatch state.
- **Facts:** describe/history/current queue snapshots. **Interpretation:** portable
  Temporal-semantic findings only. Dependency recovery and company policy are
  external conclusions.
- **Gateway calls:** `DescribeWorkflowExecution`; bounded
  `GetWorkflowExecutionHistory`; `DescribeTaskQueue` for the workflow queue and
  each activity queue explicitly named by a current pending activity.
- **Completeness:** separate history and queue completion; any causal summary must
  state if evidence is truncated.
- **Limits/refusals:** bounded pages/events/queues; competing hypotheses permitted;
  decline retry advice without recovery/idempotency evidence; current pollers do
  not prove historical health. If an activity queue is absent from describe and
  bounded history, leave it unknown; never substitute the workflow task queue based
  on an equal or convenient name.
- **Errors:** incomplete history and shared workflow/connection/budget errors;
  partial evidence should normally return findings plus warnings.
- **Scenario assertions:** age over the supplied expectation yields a delayed
  finding, never a stuck verdict. A current dispatch-blocker finding requires a
  pending activity explicitly naming queue Q plus a current zero-poller ACTIVITY
  queue Q snapshot, and states that current evidence does not establish the
  historical cause or duration. Also identify EnrichProduct retry exhaustion and catalog 503
  as high-confidence facts; report current relevant pollers; return retry
  assessment `unknown` until separate recovery evidence is supplied.
- **Schema-v1 compatibility:** adds request and response fields plus new finding
  codes under schema v1; existing field meanings are unchanged.
- **Open decisions:** whether retry assessment should be removed entirely in favor
  of findings. Recommended: keep the four-state assessment for contract testing.
