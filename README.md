# Temporal Ops MCP

Connect your MCP client to a Temporal environment and investigate workflows in natural language.
Temporal Ops MCP gives an agent read-only access to workflow state, history, task queues, schedules,
and bounded incident analysis without giving it permission to mutate Temporal.

> [!NOTE]
> This project is currently a prototype. The recommended transport is local stdio. Authentication
> for the optional HTTP transport is not implemented yet.

## Get started

Requirements: Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

### 1. Install the server

```bash
git clone https://github.com/temporalio/operator-mcp.git
cd operator-mcp
uv sync
```

### 2. Configure your Temporal environment

Copy the target configuration template and replace its placeholder values:

```bash
cp config/targets.example.yaml config/targets.yaml
```

Each key under `targets` is a user-chosen target name. The name identifies the environment in MCP
prompts and does not need to match the Temporal Namespace. Optional aliases let people use other
familiar names for the same target.

The template connects to Temporal Cloud with an API key:

```yaml
# config/targets.yaml
groups:
  # Local access group selected by OPS_MCP_GROUP; it is not sent to Temporal.
  operators:
    targets:
      # Canonical target name returned to the agent and reused in MCP tool calls.
      my-cloud-target:
        # Human-friendly resolution names; the first alias is the display name.
        aliases:
          - my cloud target
        # Optional labels returned to the agent; omit them when they are not useful.
        # account: "<account-label>"
        # cluster: "<cluster-label>"
        # Real Temporal Cloud Namespace and frontend endpoint.
        namespace: "<namespace>.<account-id>"
        address: "<namespace>.<account-id>.tmprl.cloud:7233"
        tls: true
        # Name of the environment variable containing the key, not the key itself.
        api_key_env: TEMPORAL_API_KEY
```

The target fields have three different roles:

| Field | What it represents | Used for |
| --- | --- | --- |
| Group key, such as `operators` | Local access-group name | Selects which targets the current MCP caller can access; must match `OPS_MCP_GROUP` in this prototype |
| Target key, such as `my-cloud-target` | User-chosen canonical target name | Returned to the agent as `target_ref` and reused in tool calls and target-specific policy |
| `aliases` | Optional human-friendly names | Resolving natural-language target names; the first alias is returned as the display name |
| `account` | Optional account label | Agent-visible metadata for distinguishing targets; it is not used to construct the Namespace or endpoint |
| `cluster` | Optional cluster/deployment label | Agent-visible metadata for distinguishing targets; it is not used by the Temporal connection |
| `namespace` | Exact Temporal Namespace | The real Namespace used by SDK operations; Cloud Namespaces include the account ID suffix |
| `address` | Temporal frontend gRPC address | The real server endpoint used by the SDK; it remains server-side |
| `tls` | Whether the SDK uses TLS | Connection security; it must be `true` for Temporal Cloud |
| `api_key_env` | Environment-variable name | Looks up an API key in the MCP server process without putting the secret in YAML |
| `client_cert_path`, `client_key_path` | Readable PEM file paths | Supplies the client identity for mTLS; the paths and file contents remain server-side |
| `server_ca_cert_path`, `server_name` | Optional private-CA and TLS-name overrides | Nonstandard or private deployments; normally omitted for `*.tmprl.cloud` |

API key configuration stores only the name of the credential environment variable. Keep the key
itself in your MCP client's secret store or process environment:

```bash
export TEMPORAL_API_KEY="..."
```

To connect to Temporal Cloud with mTLS instead, remove `api_key_env` and configure the client PEM
file paths:

```yaml
groups:
  operators:
    targets:
      my-cloud-target:
        aliases:
          - my cloud target
        # Optional labels returned to the agent; omit them when they are not useful.
        # account: "<account-label>"
        # cluster: "<cluster-label>"
        # These values must identify the real Temporal Cloud Namespace and endpoint.
        namespace: "<namespace>.<account-id>"
        address: "<namespace>.<account-id>.tmprl.cloud:7233"
        tls: true
        # Files containing the working client certificate and its private key.
        client_cert_path: /absolute/path/to/client.pem
        client_key_path: /absolute/path/to/client.key
```

`client_cert_path` and `client_key_path` must be configured together. All certificate paths are
read by the server when it first connects to the target; their contents are never exposed through
MCP tools. The MCP server process must be able to read both files. Restart the MCP server after
replacing either file so it creates a new Temporal connection with the rotated credentials.

For the standard `*.tmprl.cloud` endpoint, leave `server_ca_cert_path` and `server_name` unset. For
a self-hosted cluster, set `address` to its frontend host and port, choose the appropriate `tls`
value, and omit authentication fields the cluster does not require. Private deployments can also
set these optional TLS fields:

```yaml
        # Server certificate signed by a private CA:
        server_ca_cert_path: /absolute/path/to/server-ca.pem
        # Certificate/SNI name override when the dialed host uses a different name:
        server_name: temporal.example.com
```

You can add more targets under `targets` and give each one aliases that match how your team refers
to the environment.

### 3. Add the server to your MCP client

Use the equivalent of this configuration in your client, replacing the paths with absolute paths
to your checkout:

```json
{
  "mcpServers": {
    "temporal-ops": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/operator-mcp",
        "run",
        "ops-mcp"
      ],
      "env": {
        "OPS_MCP_TARGET_CONFIG": "/absolute/path/to/operator-mcp/config/targets.yaml",
        "OPS_MCP_GROUP": "operators"
      }
    }
  }
}
```

When using API-key authentication, make sure the MCP server process also receives
`TEMPORAL_API_KEY` (or whichever variable you named in `api_key_env`). How secrets are supplied
varies by MCP client. This environment variable is not needed when using mTLS.

`OPS_MCP_GROUP` is the prototype's temporary access-group selector. It must match a group in
the target configuration. A real identity provider is planned before authenticated remote use.

### 4. Start asking questions

Try prompts like:

- “What Temporal environments can I access?”
- “Check the connection to my cloud target.”
- “Why is workflow `<workflow-id>` stuck?”
- “Find failed `<workflow-type>` runs from the last hour and summarize the common failures.”
- “Is the `<task-queue>` task queue backed up or missing workers?”
- “How many workflows were affected during the incident from 14:00 to 14:30 UTC?”
- “Show me the recent failures for workflow `<workflow-id>` without dumping its full history.”
- “Prepare a reset plan for this workflow. Do not execute anything.”

The server tells the agent to resolve a configured target first, reuse that target throughout the
investigation, and prefer purpose-built diagnostics over assembling many low-level reads.

## What it can investigate

Temporal Ops MCP supports:

- target discovery and connection checks;
- workflow search, counts, descriptions, results, and focused history slices;
- task-queue health, schedules, batch operations, and approved workflow queries;
- diagnosis of a single workflow execution;
- bounded incident and population analysis with explicit coverage;
- recovery assessment and read-only workflow reset planning.

Workflow queries are denied by default. To approve specific no-argument queries for a target, add
an environment variable to the server process:

```bash
export OPS_MCP_QUERY_APPROVALS_JSON='{"my-cloud-target":["current_state","blocked_on"]}'
```

## Safety model

- **Read-only by construction.** The server does not execute Temporal mutations. A reset plan may
  include a reviewable CLI command, but reports `executed: false` and never runs it.
- **Credentials stay server-side.** Tool arguments and responses never contain Temporal addresses,
  TLS settings, certificate contents, private keys, or API keys.
- **Investigations are bounded.** Large searches use explicit limits and resumable continuation
  tokens instead of silently scanning an entire namespace.
- **Pagination retains its request scope.** Reuse the original request parameters with a
  continuation token. Visibility and ordinary history page boundaries carry Temporal's opaque
  cursor. If Temporal returns an indivisible history batch larger than the requested slice, the
  server uses a checksummed, scope-bound token to resume within that native page. Composite
  investigation tokens likewise carry validated, checksummed progress state.
- **Partial evidence stays visible.** Results distinguish candidates from confirmed workflows and
  report failed reads, sampling, and truncation rather than presenting incomplete data as exact.

## Current limitations

Stdio is the supported integration path for this prototype. It relies on the local MCP client as
the transport boundary and uses `OPS_MCP_GROUP` as a temporary identity source.

The optional streamable HTTP transport is unauthenticated and intended only for loopback testing.
Do not expose it to an untrusted network.

## Documentation and development

- [Reviewer guide](docs/reviewer-guide.md): concise concepts and code map.
- [Architecture](docs/architecture.md): component responsibilities and dependency rules.
- [Tool surface](docs/tool-surface-v2.md): routing, evidence, and continuation semantics.
- [Contract index](docs/contracts/README.md): request and response behavior for each tool.
- [Scenario matrix](docs/scenario-matrix.md): capability and evidence coverage.
- [Live test infrastructure](docs/test-infrastructure.md): optional local demos and acceptance runs.

Run the normal test suite with:

```bash
uv run pytest -m "not live_temporal"
```
