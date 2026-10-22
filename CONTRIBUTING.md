# Contributing

## Issues

Open a GitHub issue for bugs, feature requests and questions. Search the existing issues first, and if one already covers yours, add your details there instead. A useful bug report says what you did and what happened, and includes the release or commit you're running, whether the target is Temporal Server or Temporal Cloud, the MCP client, the logs and the relevant parts of your config.

## Pull requests

Open an issue to discuss a substantial change, such as a new tool or config field, before you start on it. Small fixes can go straight to a pull request.

Keep each pull request to one logical change, include tests for behavior changes, and update [docs/config.md](docs/config.md) when you add or change a config field. The README's [Build from source](README.md#build-from-source) section and the [Lint](#lint) and [Test](#test) sections below have the commands, and CI must pass before a pull request can be merged.

### Lint

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

### Test

CI runs the tests with the race detector, which does not cover the `temporal-operator-mcp` binary that the tests in `main_test.go` build and run:

```sh
go test -race ./...
```

## AI-assisted contributions

Using AI tools is fine, and you are responsible for everything you submit. You must understand the change well enough to explain it and respond to review yourself, and we may close a pull request whose discussion appears entirely AI-driven. Review generated code and docs before you open the pull request, and keep them concise and scoped, without verbose prose, unneeded comments or broad rewrites.

## Contributor License Agreement

All contributors must complete the Temporal Contributor License Agreement (CLA) before changes can be merged. A link to the CLA will be posted in the pull request.

## Security

Do not open a public issue for a security vulnerability. [SECURITY.md](SECURITY.md) explains how to report one.
