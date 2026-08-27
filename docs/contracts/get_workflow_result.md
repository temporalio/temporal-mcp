# `get_workflow_result`

- **Operator question:** What result or terminal failure did this run produce?
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`; `wait: bool = false`;
  optional bounded `wait_timeout_seconds` only when `wait=true`.
- **Output:** `state: running | completed | failed | canceled | terminated |
  timed_out | continued_as_new`, bounded decoded result or structured terminal
  failure, and successor run ID when applicable.
- **Facts:** closing history event. **Interpretation:** none.
- **Gateway calls:** `GetWorkflowExecutionHistoryReverse` to find the closing event;
  bounded long-poll only when explicitly requested.
- **Completeness:** `complete` for a terminal result; running-without-wait is also a
  complete answer to current state.
- **Limits/refusals:** default never waits; enforce wait and payload ceilings;
  require exact run ID to avoid following a changing chain implicitly.
- **Errors:** workflow/run not found, deadline, codec/decode, result-size, shared.
- **Scenario assertions:** running run returns immediately; completed run decodes
  result; failed order returns failure without waiting.
- **Open decisions:** whether v1 should optionally follow continue-as-new chains.
