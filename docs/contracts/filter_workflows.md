# `search_workflow_runs`

- **Operator question:** Which workflow runs match this visibility query?
- **Inputs:** `target_ref`, `query: str`, `fields: list[WorkflowField]`,
  `custom_search_attributes?: list[str]`, `system_search_attributes?: list[str]`,
  `page_size: 1..200`, `continuation_token?: opaque str`. Custom attributes must be registered
  and are opt-in. The bounded system projection currently supports `TemporalReportedProblems`
  without a schema lookup.
- **Output:** projected workflow rows, `returned`, observation time, and a signed opaque
  `continuation_token` bound to target, query, fields, projections, and page size.
- **Facts:** visibility records only. **Interpretation:** none.
- **Gateway calls:** on a custom-projection schema-cache miss, one namespace-read
  `WorkflowService.GetSearchAttributes` call, then one page of `ListWorkflowExecutions`;
  `query` is passed to Temporal unchanged. Concurrent misses are single-flight and the cache is
  isolated by the complete connection descriptor, including namespace and credential generation.
  System projections use a pinned local registry and do not perform schema discovery.
  Because `GetSearchAttributes` returns all legal keys without provenance, only the dedicated
  custom-projection validation boundary consults a maintained predefined/default registry and
  conservatively rejects unknown future `Temporal*` names.
- **Completeness:** `complete` for the requested page; presence of a next token is
  explicit. Never imply all matches were returned.
- **Limits/refusals:** field allowlist, at most ten explicitly named custom
  attributes, bounded strings/lists with truncation hashes, page ceiling, and
  output-size ceiling; no raw payloads or expose-all mode. Unknown/unregistered
  names fail before listing. Preserve run IDs because workflow IDs may have multiple runs.
- **Errors:** `invalid_visibility_query`, `invalid_argument`, shared target and
  connection errors. Unterminated quoted literals are rejected locally as `invalid_argument`
  before an RPC. `invalid_visibility_query` includes a bounded, sanitized
  Temporal rejection reason so the caller can revise the query.
- **Scenario assertions:** `OrderId = "88231"` returns the failed run with only
  requested fields; a second page can be fetched without duplicate first-page
  rows.
- **Query capability boundary:** `query` is Temporal Visibility Query syntax. The
  selected Temporal target—not this MCP server—decides which grammar, operators,
  and ordering clauses it supports. Callers may retry with a revised query after
  an `invalid_visibility_query` response; the MCP server does not emulate or
  locally sort unsupported expressions.
