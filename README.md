# operator-mcp

`operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP. It identifies itself to agents as `temporal-operator`.

## Prerequisites

- Go 1.27 or later (see `go.mod`).
- Optional: the `temporal` CLI on your `PATH`, which the end-to-end tests in `internal/server` use to start a Temporal dev server. Without it, the first test run downloads the CLI.

## Build

```sh
go build -o bin/ .
```

This writes `bin/operator-mcp`.

## Auth

Each issuer's `audience` must identify this server as an API, such as its resource URL or API identifier, never the client app users sign in with. ID tokens issued to that app have the same `aud`, so they would pass as access tokens. With Microsoft Entra ID v2, the audience is the client ID of the API's app registration, which must be a different registration from the one the MCP client signs in with.

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

With the race detector, which does not cover the `operator-mcp` binary that the tests in `main_test.go` build and run:

```sh
go test -race ./...
```
