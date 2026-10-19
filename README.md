# temporal-operator-mcp ([Pre-release])

`temporal-operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP or, for one developer, over stdio. Its tools let an agent list the namespaces it can read and check that the server can reach each one, read a workflow run's state and event history, see a task queue's pollers and backlog, list a namespace's search attributes, and search and count workflow runs with Visibility queries.

> [!NOTE]
>
> **Pre-release:** This project is under active development and evolving quickly. It is not ready for production use.
> Open a GitHub issue if you have questions or want to follow along.
> If you're evaluating this project for your organization, please reach out to your Temporal representative for
> guidance and to share feedback.

## Install

Each [release](https://github.com/temporalio/temporal-operator-mcp/releases) has archives for Linux (`linux`) and macOS (`darwin`) on amd64 and arm64. Download the one for your platform, check that GitHub Actions built it from this repository, and extract `temporal-operator-mcp`. For Linux on amd64:

```sh
gh release download --repo temporalio/temporal-operator-mcp --pattern '*_linux_amd64.tar.gz'
gh attestation verify temporal-operator-mcp_*_linux_amd64.tar.gz --repo temporalio/temporal-operator-mcp
tar -xzf temporal-operator-mcp_*_linux_amd64.tar.gz temporal-operator-mcp
```

## Build from source

With Go 1.27.2 or later:

```sh
go build -o bin/ .
```

This writes `bin/temporal-operator-mcp`.

## Configure

`temporal-operator-mcp` reads one YAML config, passed with `--config`. [docs/config.md](docs/config.md) documents every field, and [examples/](examples) has working configs to start from.

## Run

Over HTTP, MCP is served at `/mcp` on the config's `listen` address:

```sh
bin/temporal-operator-mcp --config examples/http.yaml
```

With `--stdio`, `temporal-operator-mcp` serves MCP over stdin and stdout to the MCP client that launches it, such as Claude Code with this `.mcp.json`, which passes the API key that [examples/stdio.yaml](examples/stdio.yaml) reads from `PAYMENTS_STAGING_API_KEY`:

```json
{
  "mcpServers": {
    "temporal-operator": {
      "command": "/path/to/temporal-operator-mcp",
      "args": ["--stdio", "--config", "/path/to/config.yaml"],
      "env": {"PAYMENTS_STAGING_API_KEY": "${PAYMENTS_STAGING_API_KEY}"}
    }
  }
}
```

Other MCP clients take the same command, args and env in their own config format, each with its own syntax for reading an environment variable, if it has one. For a client that has none, or that does not see the variable because it was started outside a shell, have the target read its key from a file with `api_key: {file: /path/to/key}` instead. Otherwise the server gets no key and does not start, or gets the `${...}` text as the key, and every call to the target fails, which `check_connection` shows.

## Audit records and logs

`temporal-operator-mcp` writes an audit record of each tool call that passes the token check to stdout, as one line of JSON, and its logs to stderr. With `--stdio`, both go to stderr, and each record's `issuer` and `subject` are null because there is no token. [`audit_log_file` and `error_log_file`](docs/config.md#audit_log_file-and-error_log_file) send the audit records and the logs to files instead.

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
