# `inspect_activity_queue_dependencies`

- **Operator question:** Which long-running workflows in a defined population look
  unable to make progress, and does current dispatch evidence suggest worker or
  workflow-side causes?
- **Inputs:** `target_ref`, required visibility query/time window,
  `minimum_open_duration`, explicit run/history/queue budgets, optional explicit
  `activity_task_queue` confirmation criterion, and up to four registered custom
  search attributes as grouping dimensions.
- **Output:** population/candidate counts, sampled run findings, deduplicated task
  queue snapshots, wait-state evidence, full inspection report, and—when an
  activity queue criterion is supplied—a blast-radius summary with candidate
  frame, inspected, confirmed, unconfirmed, failed, uninspected, completeness,
  and confirmed-only workflow-type groups.
  When custom aggregation dimensions are requested, typed groups include only
  evidence-confirmed affected runs; uninspected candidates are never promoted.
  Aggregation scope is labeled complete or bounded.
- **Facts:** open durations, pending state, current pollers/backlog, optional
  workflow query result. **Interpretation:** candidate categories such as
  `waiting_normally`, `dispatch_not_observed`, `workflow_task_repeatedly_failing`,
  or `unknown`; not a universal stuck verdict.
- **Gateway calls:** multi-page bounded `ListWorkflowExecutions`; deduplicated `DescribeTaskQueue`;
  bounded `DescribeWorkflowExecution`; optionally bounded history/query calls when
  the approved contract requires them.
- **Completeness:** candidate collection and run inspection are reported separately;
  sampled unless every collected candidate is inspected. Queue snapshots
  are current and timestamped separately.
- **Limits/refusals:** duration alone cannot establish stuck; distinguish workflow
  and activity queues; bind an activity-queue claim to pending-activity evidence
  for that exact run; bound fan-out; refuse unbounded population queries.
- **Errors:** invalid query, deadline/budget, query errors, shared; return partial
  classified candidates with caveats when safe.
- **Scenario assertions:** a signal-waiting workflow is not labeled broken; missing
  activity pollers are distinguished from workflow pollers; repeated workflow-task
  failures yield a code-problem finding; sample size is explicit.
- **Prohibited join:** visibility `TaskQueue` is the workflow task queue. A workflow
  using workflow task queue `ledger` is not thereby proven to be waiting on the
  `ledger` activity task queue. Group and count only confirmed affected runs;
  report uninspected and failed runs separately.
- **Schema-v1 compatibility:** adds optional `activity_task_queue`, per-run
  `activity_queue_membership`, and optional `blast_radius` fields under schema v1.
  Existing `population_count` remains an approximate candidate count and is never
  reinterpreted as an affected count.
- **Open decisions:** exact minimum evidence required before the word `stuck` may
  appear in a finding. Recommended: keep it only in the tool/operator question.
