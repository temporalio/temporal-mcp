# `inspect_workflow_task_failures`

`find_workflow_task_failures` was a pre-release compatibility alias. The public tool is now
`inspect_workflow_task_failures`; this sheet remains as contract history for the underlying
service semantics.

- **Operator question:** Within a defined workflow population, which inspected runs
  show workflow-task failures such as non-determinism?
- **Inputs:** `target_ref`, required visibility query/time window; optional failure
  class and up to four registered `group_by_custom_search_attributes`; explicit
  candidate/page/run/history limits, and sampling strategy.
- **Output:** population visibility count when queried, narrowed candidate count,
  sampled/inspected run findings, failure-class groups, opt-in bounded aggregation
  by workflow type and up to four registered custom attributes, and full inspection report.
  Aggregation scope is `complete_query_population` only for a fully collected and
  inspected query; otherwise it is `bounded_inspection`.
- **Facts:** counts, visibility rows, describe/history events. **Interpretation:**
  classification of observed workflow-task failures; no prevalence claim outside
  the inspected sample.
- **Gateway calls:** `CountWorkflowExecutions`, multi-page bounded
  `ListWorkflowExecutions`, then bounded `DescribeWorkflowExecution` and reverse
  `GetWorkflowExecutionHistory` fan-out. Reverse reads inspect the most recent
  history region first; the ledger states when older history was truncated.
- **Completeness:** normally `sampled`; `complete` only if every narrowed candidate
  is inspected within budget and the query population is bounded.
- **Limits/refusals:** each call has bounded candidate, page, run, and history budgets;
  a signed continuation advances to subsequent native Visibility frames without a
  global candidate ceiling. Fan-out remains independently bounded. Sampling occurs
  only after candidate collection. A truncated frame is explicitly
  nonrepresentative and cannot support extrapolation. A fully collected frame with
  a fully inspected deterministic hash sample is marked estimate-eligible, but the
  server never calculates an affected-population estimate.
- **Errors:** invalid query, deadline/budget, incomplete history, shared; partial
  results remain usable with explicit completion state.
- **Scenario assertions:** 100,000-run population narrows before history reads;
  a 1,900-run narrowed population spanning ten pages is collected before sampling;
  a first page containing only one workflow type cannot bias the sample;
  reported sample size equals inspected runs; known non-determinism fixtures are
  classified with exact event/message and bounded stack-trace evidence; answer never
  invents a source branch/cause or says all failures were found when sampled.
- **Open decisions:** default sampling method and minimum useful sample size.
