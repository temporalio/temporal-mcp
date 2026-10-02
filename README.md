# operator-mcp

`operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP. It identifies itself to agents as `temporal-operator`. It is written in Go.

## Prerequisites

- Go 1.27 or later (see `go.mod`).
- Optional: the `temporal` CLI on your `PATH`, which the end-to-end tests use to start a Temporal dev server. Without it, the first test run downloads the CLI.

## Build

```sh
go build -o bin/ .
```

This writes `bin/operator-mcp`.

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

```sh
go test ./...
```

With the race detector, which covers the in-process tests but not the server binary that the end-to-end tests start:

```sh
go test -race ./...
```

The end-to-end tests in `main_test.go` build the `operator-mcp` binary, start a Temporal dev server, and talk to the running MCP server over HTTP.
