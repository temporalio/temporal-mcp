# `describe_workflow`

- **Operator question:** What is the current/closed state of this exact workflow run?
- **Inputs:** `target_ref`, `workflow_id`, `run_id?: str`; omission means the
  Temporal-resolved current run and the output must include its run ID.
- **Output:** execution identity/type/status, workflow task queue, timestamps,
  history length, retry/run linkage, pending activities, parent/child linkage
  available from the describe response, and bounded decoded memo/search
  attributes. Each pending activity exposes its activity task queue from
  `activity_options`, state, attempt/max attempts, scheduled/last-started times,
  last heartbeat time, and bounded decoded heartbeat details when present.
- **Facts:** `DescribeWorkflowExecution` fields. **Interpretation:** none.
- **Gateway calls:** `DescribeWorkflowExecution`.
- **Completeness:** `complete` for describe facts, with payload field-level decode
  and truncation markers.
- **Limits/refusals:** payload/output ceiling; failed heartbeat-detail decoding
  returns redacted field status, not raw bytes; absence of a heartbeat timestamp
  means "not reported," not "activity dead"; do not claim complete transitive
  child topology.
- **Errors:** workflow/run not found, codec/decode, result-size, shared errors.
- **Scenario assertions:** failed order reports its exact run, queue, failed status,
  history length, and no fabricated pending activity. Session 2A reports the
  pending `FetchLedgerBatch` activity's activity queue, attempt, and heartbeat
  evidence without confusing them with workflow-task-queue facts.
- **Schema-v1 compatibility:** the pending-activity evidence above is added as
  always-present fields under the existing `schema_version: "1"` response;
  timestamps/queue may be null and heartbeat details carry explicit decode
  status. This is a deliberate prototype break for strict v1 consumers. Existing
  fields retain their meanings; `task_queue` remains the workflow task queue.
- **Open decisions:** exact child-link semantics available from pinned API.
