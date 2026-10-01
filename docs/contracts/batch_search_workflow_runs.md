# `batch_search_workflow_runs`

- **Operator question:** Which runs match the same bounded query across a known set of targets?
- **Inputs:** one to twenty unique authorized `target_refs`, a shared Visibility `query`, projection,
  and page size. Continuations are deliberately handled by the single-target tool.
- **Output:** one ordered item per requested target. Each item contains exactly one complete
  single-page search response or one structured safe error; one target failure never discards the
  other outcomes.
- **Execution:** shared query/projection validation occurs before fan-out. Target authorization is
  still performed independently, and at most four target searches run concurrently. Each item has
  scoped accounting while all items atomically consume the same aggregate request budget.
- **Completeness:** each success describes one bounded page and includes its own Temporal-native
  continuation. Visibility rows and approximate counts are separate, eventually consistent
  observations.
- **Errors:** malformed shared inputs fail the whole request before any target call. Authorization,
  permission, connectivity, and target-specific schema errors are preserved on their item.
  Unexpected exceptions become safe per-target failures and are logged with only request ID, safe
  target reference, operation, and traceback metadata—never query text, credentials, or endpoints.
