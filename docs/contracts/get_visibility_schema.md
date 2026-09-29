# `get_visibility_schema`

- **Operator question:** Which workflow visibility fields are registered on this
  namespace, and what are their indexed types?
- **Inputs:** `target_ref: str`.
- **Output:** ordered legal fields with name and indexed value type. The response deliberately
  makes no system/custom provenance claim because this read API does not provide one.
- **Facts:** registered search attributes. **Interpretation:** none. This tool does
  not discover or claim query grammar, operator, grouping, or ordering support.
- **Gateway calls:** the shared, cached Namespace Read-authorized
  `WorkflowService.GetSearchAttributes` path also used by projection and grouping validation.
- **Completeness:** `complete` for returned namespace schema; `unknown` if the
  server cannot expose the schema.
- **Limits/refusals:** do not invent fields from observed executions or infer
  query capabilities from an indexed type.
- **Errors:** shared target, connection, namespace, capability, deadline errors.
- **Scenario assertions:** configured `OrderId: Keyword` appears; a nonexistent
  field is absent rather than guessed; no operator capability matrix is returned.
- **Query capability boundary:** the selected Temporal target validates visibility
  query grammar and operators when a query is executed.
