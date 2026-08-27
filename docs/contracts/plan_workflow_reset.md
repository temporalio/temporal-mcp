# `plan_workflow_reset`

- **Operator question:** What reset points exist for this exact run, what would
  replay, and can the server safely recommend one?
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`; optional operator
  rationale used only as labeled context, never as evidence.
- **Output:** observed run/history identity; all supported valid reset points with
  plain labels; replay-impact facts; safety findings; optional recommended point;
  advisory `recheck_after`; and `command_preview {shell, command, executed:false}`.
- **Facts:** history event topology and effects before/after each point.
  **Interpretation:** point labels, replay impact, universal safety findings, and an
  optional recommendation. External side-effect safety is unknown unless Temporal
  history directly establishes it.
- **Gateway calls:** `DescribeWorkflowExecution`, complete bounded
  `GetWorkflowExecutionHistory`.
- **Completeness:** recommendation requires complete history through the observed
  length. Otherwise return no recommendation and `incomplete_history` warning.
- **Limits/refusals:** require run ID; never execute; refuse recommendation for
  unsupported events, ambiguous parallel work, child/external interactions,
  continue-as-new boundaries, versioning uncertainty, or unknown replay effects;
  still return supported facts/points when safe.
- **Errors:** workflow/run not found, incomplete history, result too large,
  `unsafe_to_recommend`, shared. A safe no-recommendation answer is not an error.
- **Scenario assertions:** linear order history recommends the point before
  EnrichProduct, identifies completed payment and reapplied signal, pins run and
  observed history length, and emits a one-line unescaped non-executed preview;
  parallel/child histories return no recommendation.
- **Open decisions:** Temporal's exact valid reset-point event classes and CLI
  event-ID semantics must be pinned with an integration test before approval.
