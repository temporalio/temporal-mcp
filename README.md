# temporal-operator-mcp ([Pre-release])

`temporal-operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP or, for one developer, over stdio. Its tools let an agent list the namespaces it can read and check that the server can reach each one, read a workflow run's state and event history, see a task queue's pollers and backlog, list a namespace's search attributes, and search and count workflow runs with Visibility queries.

> [!NOTE]
>
> **Pre-release:** This project is under active development and evolving quickly. It is not ready for production use.
> Open a GitHub issue if you have questions or want to follow along.
> If you're evaluating this project for your organization, please reach out to your Temporal representative for
> guidance and to share feedback.

## Get started

These steps set up `temporal-operator-mcp` for one developer, whose agent launches it and talks to it over stdio.

### Download

Each [release](https://github.com/temporalio/temporal-operator-mcp/releases) has an archive for each platform: `darwin_arm64` for Apple silicon Macs, `darwin_amd64` for Intel Macs, and `linux_amd64` and `linux_arm64`. Its "Source code" links are a copy of this repository, not the server. Set `platform` to yours, then download the archive, check that GitHub Actions built it from this repository, and extract `temporal-operator-mcp`:

```sh
platform=darwin_arm64
gh release download --repo temporalio/temporal-operator-mcp --pattern "*_$platform.tar.gz"
gh attestation verify temporal-operator-mcp_*_$platform.tar.gz --repo temporalio/temporal-operator-mcp
tar -xzf temporal-operator-mcp_*_$platform.tar.gz temporal-operator-mcp
```

To download a release that is still a draft, put its tag after `download`.

On macOS, download with `gh` as above. A browser marks what it downloads with the `com.apple.quarantine` flag, and macOS won't run a binary with that flag unless it's notarized, which this one isn't. To run a binary from a browser download, remove the flag:

```sh
xattr -d com.apple.quarantine temporal-operator-mcp
```

### Configure

`temporal-operator-mcp` reads one YAML config, passed with `--config`. Copy [examples/stdio.yaml](examples/stdio.yaml) and change its targets to the Temporal namespaces you want the agent to read. [docs/config.md](docs/config.md) documents every field.

### Connect

Add `temporal-operator-mcp` to your agent with `--stdio`, your config and the name and value of each environment variable your config reads with `env:`. For Claude Code, with `--scope user` so the server is available in every project:

```sh
claude mcp add --scope user temporal-operator -e KEY=value -- /path/to/temporal-operator-mcp --stdio --config /path/to/config.yaml
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
