# temporal-operator-mcp ([Pre-release])

`temporal-operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP or, for one developer, over stdio. Its tools let an agent list the namespaces it can read and check that the server can reach each one, read a workflow run's state and event history, see a task queue's pollers and backlog, list a namespace's search attributes, and search and count workflow runs with Visibility queries.

> [!NOTE]
>
> **Pre-release:** This project is under active development and evolving quickly. It is not ready for production use.
> Open a GitHub issue if you have questions or want to follow along.
> If you're evaluating this project for your organization, read the [request for comment](REQUEST_FOR_COMMENT.md) and
> reach out to your Temporal representative for guidance and to share feedback.

## Get started

These steps set up `temporal-operator-mcp` for one developer, whose agent launches it and talks to it over stdio.

### Install

```sh
curl -sSfL https://raw.githubusercontent.com/temporalio/temporal-operator-mcp/main/install.sh | sh
temporal-operator-mcp --version # checks the install
```

### Configure

`temporal-operator-mcp` reads one YAML config, passed with `--config`. Copy [examples/stdio.yaml](examples/stdio.yaml) and change its targets to the Temporal namespaces you want the agent to read. [docs/config.md](docs/config.md) documents every field.

### Connect

Add `temporal-operator-mcp` to your agent with `--stdio`, your config and the name and value of each environment variable your config reads with `env:`. For Claude Code, with `--scope user` so the server is available in every project:

```sh
claude mcp add --scope user temporal-operator -e KEY=value -- temporal-operator-mcp --stdio --config /path/to/config.yaml
```

For Codex:

```sh
codex mcp add temporal-operator --env KEY=value -- temporal-operator-mcp --stdio --config /path/to/config.yaml
```

Other agents take the same command, arguments and environment variables in their own config format.

### Check

Ask the agent to check its connection to Temporal. It calls `check_connection`.

## Run a shared server

For a team, run `temporal-operator-mcp` as one HTTP server whose users sign in with an OpenID Connect (OIDC) identity provider. Start from [examples/http-auth.yaml](examples/http-auth.yaml). The [`auth`](docs/config.md#auth) and [`authorization`](docs/config.md#authorization) sections of docs/config.md cover how users sign in and which tools and targets each one gets.

Without `--stdio`, `temporal-operator-mcp` serves MCP at `/mcp` on the config's `listen` address:

```sh
./temporal-operator-mcp --config config.yaml
```

## Audit records and logs

`temporal-operator-mcp` writes an audit record of each tool call that passes the token check to stdout, as one line of JSON, and its logs to stderr. With `--stdio`, both go to stderr, and each record's `issuer` and `subject` are null because there is no token. [`audit_log_file` and `error_log_file`](docs/config.md#audit_log_file-and-error_log_file) send the audit records and the logs to files instead.

## Build from source

With Go 1.27.2 or later:

```sh
go build -o bin/ .
```

This writes `bin/temporal-operator-mcp`.

## Shutdown

On SIGINT or SIGTERM, `temporal-operator-mcp` stops accepting new requests and waits up to `shutdown_timeout` for the tool calls in progress to finish. With `--stdio`, closing stdin also starts this shutdown, and stdout stays open for the calls' responses:

```yaml
shutdown_timeout: 25s # 8s if unset
```

It then cancels the calls still running, which get audit records with outcome `cancelled`, and exits within another second. Set `shutdown_timeout` at least a second shorter than the time your platform allows between SIGTERM and SIGKILL.

Calls interrupted by shutdown return a tool error with code `server_shutting_down`, message `the server is shutting down`, and `retryable: true`.

A second signal stops `temporal-operator-mcp` at once. A call cut off that way, or by SIGKILL, gets no audit record.

## Lint

CI runs `go vet` and a `gofmt` check:

```sh
go vet ./...
test -z "$(gofmt -l .)"
```

If the `gofmt` check fails, `gofmt -l .` lists the unformatted files and `gofmt -w .` fixes them.

Locally we also run staticcheck and govulncheck:

```sh
go run honnef.co/go/tools/cmd/staticcheck@latest ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

## Test

CI runs the tests with the race detector, which does not cover the `temporal-operator-mcp` binary that the tests in `main_test.go` build and run:

```sh
go test -race ./...
```

[Pre-release]: https://docs.temporal.io/evaluate/development-production-features/release-stages
