# `query_workflow`

- **Operator question:** What does this running workflow report for one approved,
  no-argument diagnostic query?
- **Inputs:** exact `target_ref`, `workflow_id`, `run_id`, and required approved
  `query_name`; no query arguments. Use `list_approved_workflow_queries` for discovery.
- **Output:** `answer.kind=result` contains the bounded
  decoded query result, query name, run identity, and observed time.
- **Facts:** workflow-provided query result. **Interpretation:** none; the server
  does not claim the result is truthful or a generic "blocked on" state.
- **Gateway calls:** `QueryWorkflow` after checking a
  per-target approved-name list for execution.
- **Completeness:** `complete` for the query response or `unknown` on query failure.
- **Limits/refusals:** exact allowlist, deadline/output ceiling, no arguments, no
  unapproved built-in or arbitrary query names, no retry loop. Approved built-in
  SDK diagnostic queries are allowed by exact name; they are not globally enabled.
- **Errors:** `query_not_approved`, `query_failed`, workflow/run not found,
  codec/decode, deadline, shared.
- **Scenario assertions:** `list_approved_workflow_queries` deterministically lists approved
  names as configuration, not proven run support, and makes no RPC. Approved
  Python SDK built-in `__stack_trace` succeeds
  while a compatible workflow worker is available; unknown query is refused before
  an RPC; closed/unavailable workflow response is accurately classified. A stack
  trace corroborates where workflow code is awaiting but does not by itself prove
  queue health or historical cause.
- **Compatibility:** the service layer temporarily accepts the former omitted-name discovery
  request for internal callers, but the MCP schema requires `query_name`. Existing result fields
  retain their meanings under `kind: result`.
