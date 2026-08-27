# `list_batch_operations`

- **Operator question:** Which namespace batch jobs exist and what progress/state
  does each have?
- **Inputs:** `target_ref`; optional state/job-type filters; `page_size`,
  `continuation_token`; `include_details: bool = true`.
- **Output:** batch summaries; when requested, bounded per-operation detail and
  progress; continuation token and inspection counts.
- **Facts:** list and describe responses. **Interpretation:** normalized progress
  only; do not infer workflow-level success from job completion.
- **Gateway calls:** paged `ListBatchOperations`, then bounded
  `DescribeBatchOperation` fan-out for returned rows.
- **Completeness:** list page and detail completeness reported separately; describe
  failures produce partial rows and `unknown`/`budget_exhausted` detail state.
- **Limits/refusals:** cap page size, detail concurrency/calls, and output; read-only
  types only.
- **Errors:** unsupported capability, invalid token/filter, deadline/budget, shared.
- **Scenario assertions:** a completed and running job have correct progress;
  detail budget exhaustion never masquerades as absent detail.
- **Open decisions:** exact state/job-type vocabulary for pinned server.
