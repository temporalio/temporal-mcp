# `assess_workflow_recovery`

`assess_workflow_recovery` is a read-only evidence composite used before reset planning. The caller
supplies one exact failed workflow/run, a comparison Visibility query, and one exact activity type.
The server pins and inspects the failed history, excludes that execution from the comparison set,
then reads bounded exact histories for the comparison runs.

The response reports candidate runs, histories inspected, activity successes, activity failures,
inspection failures, per-run outcomes, completion, and a recovery assessment. A later successful
activity outcome supports the proposition that recovery occurred; it does not directly measure or
prove dependency health. The tool neither creates a reset plan nor executes a reset.

Audit this tool from the MCP adapter in `src/ops_mcp/mcp/adapters.py`, through the request/response
models in `src/ops_mcp/contracts/recovery.py`, to the orchestration in
`src/ops_mcp/services/population.py`. Gateway calls remain behind the service boundary, and
deterministic acceptance coverage lives in `tests/test_population_services.py`.
