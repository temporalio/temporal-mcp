# Live acceptance test infrastructure

The live acceptance harness is deliberately separate from MCP server infrastructure. Every
deterministic run starts its own headless, in-memory `temporal server start-dev` child process on a
free loopback port. It generates a temporary target configuration, starts only the fixture workers
needed by the selected session, seeds the fixture, runs the agent command, and then terminates only
the child process groups it created. It never connects to, resets, or deletes the developer's usual
Temporal database.

## Deterministic session acceptance

Pass the agent invocation after `--`. The invocation inherits `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE`, and `OPS_MCP_TARGET_CONFIG`, so an MCP server started by the CLI resolves
`orders prod`, `temporal dev`, or `acceptance local` to the ephemeral server.

```bash
uv run ops-mcp-acceptance --mode isolated --session 2 -- \
  claude -p 'Is session2-payment-reconciliation-main stuck? Investigate with temporal-ops.'
```

Sessions 1 through 4 are supported. Session 3 performs the v1 seed, worker replacement, and v2
exposure sequence. Session 4 seeds while only workflow workers are available and starts recovery
activity workers afterward. Fixture and server logs live in a temporary directory for the duration
of the command and are removed after every outcome, including interruption or failure.

Use `--port PORT` only when a stable port is needed by an MCP client configuration that cannot
inherit environment variables. The harness still starts an independent server and will fail instead
of touching a process already using that port.

## Mixed-data robustness acceptance

The noisy mode starts one fresh server and seeds the ordinary fixtures for Sessions 1 through 4 in
the same `default` namespace before running the focused investigation:

```bash
uv run ops-mcp-acceptance --mode noisy --session 4 -- \
  claude -p 'Analyze the ledger incident impact. Separate candidates from confirmed impact.'
```

`--session` identifies the customer question being graded; it does not change or filter the seeded
Temporal data. No scenario identifier, test-only Search Attribute, or server-side fixture selector
is created. The agent must use real operational evidence such as workflow type, timestamps, task
queues, histories, `Team`, and `Domain` to work through unrelated workflows.

Run isolated acceptance for deterministic product checks. Run noisy acceptance separately as a
robustness evaluation; a failure there must not be hidden by adding test-specific filtering to a
production tool.
