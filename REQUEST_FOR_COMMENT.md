# Request for Comment: Temporal Ops MCP Server

> [!WARNING]
> This is a proposal. The final product may change based on feedback. Please send comments to your Temporal account team.

## What it is

An open-source MCP server that gives your AI agents read-only, governed access to Temporal. You run it in your own environment, alongside your other internal tools.

When something breaks in production, the first questions tend to be the same. Why did this workflow fail? Why is this run stuck? Did the last deploy cause non-determinism errors? Which workflows did the incident affect? With this server, an agent in Claude Code, Cursor, Codex, or your own tooling can look up workflows, search and count them by status or search attribute, and read failure details. The server can use your organizational SSO and doesn’t require distributing Temporal keys or certificates to end users.

Using the MCP server, agents never hold production credentials. The server connects with one read-only service account, and your internal identity provider decides who can use which tools on which namespaces. It also supports codec decryption for encrypted payloads.

## Become a design partner

We're building this with a small group of customers before the first release. Design partners get early builds and work directly with the team building it, and their feedback shapes what we build next. We'd ask you to set it up, use it on real issues, and tell us what works and what doesn't. The setup is described in the next section.

To start, we'd like to know:

- Can your team host and operate this server?
- Which identity provider and MCP clients or gateways do you use?
- Who inside your company do you envision using this with their agents? Which agents do they use?
- What are they trying to do? How do they do that today?
- Is anything here a blocker?

## What it requires of you

> [!IMPORTANT]
> **This is self-hosted. Temporal does not run it for you.**

### 1. Deploy the server

The server is a single binary. Download the release for your platform from the [GitHub releases page](https://github.com/temporalio/temporal-mcp/releases), put it on a host in your network, and start it with your config file:

```bash
./temporal-mcp --config config.yaml
```

To upgrade, replace the binary with a newer release and restart it. The server is fully stateless, so you can run several copies behind a load balancer.

- All settings live in one YAML config file, and the server won't start if any of it is invalid. It serves MCP over streamable HTTP/S and stdio.
- List each Temporal namespace the MCP server will connect to in the config file.
- If the config file has no `auth` section, the server listens only on localhost, which works for one developer on a laptop. For a team, add an `auth` section to the config file (step 3) and expose the server over HTTPS. You can give the server your own certificate and key, or put it behind a load balancer or reverse proxy you already use.

### 2. Give the server a Temporal service account

The server connects to Temporal as its own identity, not as the person using the agent. It can authenticate with either an API key or an mTLS client certificate. The MCP server keeps its own auditable logs.

1. Create a service account for the server in Temporal Cloud.
2. Give it Read permission on each Namespace you list as a target.
3. Create an API key for the service account, or issue an mTLS client certificate from a CA the Namespace trusts.
4. Make the credential available to the server through an environment variable or a mounted file, and reference it from each target in the config file. The server connects over TLS and never puts credentials in tool output.

A few things to know about this architectural model:

- Temporal's own audit log shows every call as coming from the service account. To see more details about the caller, use the MCP server's audit log (step 4), which records the signed-in user.
- The service account sets an upper limit on what the server can do. The MCP server’s group-based access configuration can narrow that further for each group of human users and their agents.
- API keys and certificates both expire, so plan for rotation.

### 3. Register the server in your identity provider

When the config file has an `auth` section, the server requires sign-in. Every request must include a JWT access token from an issuer listed in that section. Once the MCP server is registered with your identity provider, users should be automatically redirected to sign in to it.

In your identity provider:

1. Register the MCP server as an API in your identity provider. Give it an audience, such as `api://temporal-ops-mcp`. Access tokens must include it in the `aud` claim.
2. Allow your MCP client (Codex, Claude Code) or MCP gateway to request tokens with that audience. If the client or gateway isn't set up as an app in your identity provider yet, add it first.
3. Assign groups or app roles to the people who should have access, and include them in the access token.

> [!NOTE]
> **If you use an MCP gateway**
>
> - Add the Temporal MCP server to your gateway as a custom MCP server, using the server's URL, for example `https://temporal-mcp.example.com/mcp`.
> - Choose OAuth with per-user credentials, so each person signs in with your SSO and the server sees who is calling.
> - If the gateway is hosted outside your network, the gateway would need network access to your Temporal MCP server deployment, such as an IP allowlist or a private network connection.

### 4. Collect the audit log

The server writes one JSON line to stdout for every tool call from a signed-in user, including who made it. Writing audit logs to files instead of stdout will also be supported.

### 5. Encrypted payloads

Payloads are your application's data: workflow inputs and results, memos, heartbeat details, and failure details. Each namespace target in the config file has a `payload_handling` setting that controls how tool responses show them. By default (`omit`) they're left out. `passthrough` returns them as Temporal stores them, still encoded. `decode` sends them to your codec server, the same URL you pass to the Temporal CLI as `--codec-endpoint`. Codec servers are defined in a `codecs` section.

With `decode`, your codec server must accept requests from the MCP server itself. The server can't pass on a user's Temporal Cloud login the way the Temporal UI does, so give it its own `Authorization` header value, such as a bearer token. If a payload can't be decoded, it's left out of the response and the reason is logged.

## What it does not do (first release)

- No changes to workflows (no resets, signals, or terminations)
- No Temporal-hosted version
