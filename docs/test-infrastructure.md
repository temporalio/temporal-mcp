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
uv run python -m ops_mcp.acceptance.runner --mode isolated --session 2 -- \
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
uv run python -m ops_mcp.acceptance.runner --mode noisy --session 4 -- \
  claude -p 'Analyze the ledger incident impact. Separate candidates from confirmed impact.'
```

`--session` identifies the customer question being graded; it does not change or filter the seeded
Temporal data. No scenario identifier, test-only Search Attribute, or server-side fixture selector
is created. The agent must use real operational evidence such as workflow type, timestamps, task
queues, histories, `Team`, and `Domain` to work through unrelated workflows.

Run isolated acceptance for deterministic product checks. Run noisy acceptance separately as a
robustness evaluation; a failure there must not be hidden by adding test-specific filtering to a
production tool.

## Live mTLS connection test

The opt-in mTLS integration test requires `openssl` and a self-hosted `temporal-server` binary. It
generates a disposable CA plus CA-signed server and client certificates, then configures the real
Temporal frontend with its server certificate, `requireClientAuth: true`, and the CA in
`clientCaFiles`. It verifies that a client without a certificate is rejected, then uses the normal
target catalog and client provider to register the isolated test namespace and run live
`GetSystemInfo` and `DescribeNamespace` RPCs directly against that frontend. There is no TLS proxy
in this test.

Build `temporal-server` from a compatible checkout of the Temporal server repository, then provide
its path to the test:

```bash
OPS_MCP_MTLS_INTEGRATION=1 \
OPS_MCP_TEMPORAL_SERVER_BINARY=/absolute/path/to/temporal-server \
uv run pytest \
  tests/integration/test_mtls_client_provider_integration.py
```

All generated keys, certificates, logs, and processes are scoped to the test's temporary directory
and removed or stopped during cleanup.
