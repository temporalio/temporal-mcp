# temporal-mcp ([Pre-release])

`temporal-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP or, for one developer, over stdio. Its tools let an agent list the namespaces it can read and check that the server can reach each one, read a workflow run's state and event history, see a task queue's pollers and backlog, list a namespace's search attributes, and search and count workflow runs with Visibility queries.

> [!NOTE]
>
> **Pre-release:** This project is under active development and evolving quickly. It is not ready for production use.
> Open a GitHub issue if you have questions or want to follow along.
> If you're evaluating this project for your organization, read the [request for comment](REQUEST_FOR_COMMENT.md) and
> reach out to your Temporal representative for guidance and to share feedback.

## Get started

These steps set up `temporal-mcp` for one developer, whose agent launches it and talks to it over stdio. You try it on a local Temporal dev server first, then add your own namespaces. When you're ready, go to [Run a shared server](#run-a-shared-server) to set up this MCP server for a broader audience.

### Install

```sh
curl -sSfL https://raw.githubusercontent.com/temporalio/temporal-mcp/main/install.sh | sh
temporal-mcp --version # checks the install
```

### Start a dev server

[Install the Temporal CLI](https://docs.temporal.io/cli/setup-cli) and start a dev server, which serves a `default` namespace at `localhost:7233`:

```sh
temporal server start-dev
```

### Configure

`temporal-mcp` reads one YAML config, passed with `--config`. Copy [examples/stdio-local-config.yaml](examples/stdio-local-config.yaml), whose `local` target reads the dev server.

### Connect

Add `temporal-mcp` to your agent with `--stdio` and the absolute path of your config, since the agent starts the server from its own working directory. For Claude Code, with `--scope user` so the server is available to you in every project, instead of only in the current directory:

```sh
claude mcp add --scope user temporal -- temporal-mcp --stdio --config /absolute/path/to/config.yaml
```

For Codex:

```sh
codex mcp add temporal -- temporal-mcp --stdio --config /absolute/path/to/config.yaml
```

Other agents take the same command, arguments and environment variables in their own config format.

### Check

Ask the agent to check its connection to the `local` target. It calls `check_connection`, which reports whether the server can reach the dev server and its `default` namespace.

Once your app runs workflows on the dev server, or once you add your own namespaces, ask the agent about them, for example:

- Why is workflow `order-123` stuck?
- Which workflow runs failed in the last hour?
- Is any worker polling the `orders` task queue?

### Add your own namespaces

To read your Temporal Cloud or self-hosted namespaces, add a target for each one to your config. The `payments-staging` target in [examples/stdio-config.yaml](examples/stdio-config.yaml) shows a Temporal Cloud namespace with an API key, and [docs/config.md](docs/config.md) documents every field.

For each environment variable your config reads with `env:`, add `-e KEY=value` (Claude Code) or `--env KEY=value` (Codex) just before the `--`. To change the command, run it again: `codex mcp add` replaces the server of the same name, but for Claude Code, run `claude mcp remove temporal` first.

The server reads its config only when it starts, so restart your agent after you change the config, then ask it to check its connection to each new target.

## Run a shared server

For a team, run `temporal-mcp` as one HTTP server whose users sign in with an OpenID Connect (OIDC) identity provider. Start from [examples/http-auth-config.yaml](examples/http-auth-config.yaml). The [`auth`](docs/config.md#auth) and [`authorization`](docs/config.md#authorization) sections of docs/config.md cover how users sign in and which tools and targets each one gets.

Without `--stdio`, `temporal-mcp` serves MCP at `/mcp` on the config's `listen` address:

```sh
./temporal-mcp --config config.yaml
```

## Audit records and logs

`temporal-mcp` writes an audit record of each tool call that passes the token check to stdout, as one line of JSON, and its logs to stderr. With `--stdio`, both go to stderr, and each record's `issuer` and `subject` are null because there is no token. [`audit_log_file` and `error_log_file`](docs/config.md#audit_log_file-and-error_log_file) send the audit records and the logs to files instead.

## Build from source

With Go 1.27.2 or later:

```sh
go build -o bin/ .
```

This writes `bin/temporal-mcp`.

## Shutdown

On SIGINT or SIGTERM, `temporal-mcp` stops accepting new requests and waits up to `shutdown_timeout` for the tool calls in progress to finish. With `--stdio`, closing stdin also starts this shutdown, and stdout stays open for the calls' responses:

```yaml
shutdown_timeout: 25s # 8s if unset
```

It then cancels the calls still running, which get audit records with outcome `cancelled`, and exits within another second. Set `shutdown_timeout` at least a second shorter than the time your platform allows between SIGTERM and SIGKILL.

Calls interrupted by shutdown return a tool error with code `server_shutting_down`, message `the server is shutting down`, and `retryable: true`.

A second signal stops `temporal-mcp` at once. A call cut off that way, or by SIGKILL, gets no audit record.

[Pre-release]: https://docs.temporal.io/evaluate/development-production-features/release-stages
