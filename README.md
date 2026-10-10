# operator-mcp

`operator-mcp` is a read-only MCP server that gives AI agents access to the Temporal namespaces listed in its config, over streamable HTTP. It identifies itself to agents as `temporal-operator`.

Temporal Cloud targets with an API key also connect to the Cloud Ops API at `saas-api.tmprl.cloud:443` for `list_search_attributes` and `search_workflows`.

## Install

Each [release](https://github.com/temporalio/operator-mcp/releases) has archives for Linux (`linux`) and macOS (`darwin`) on amd64 and arm64. Download the one for your platform, check that GitHub Actions built it from this repository, and extract `operator-mcp`. For Linux on amd64:

```sh
gh release download --repo temporalio/operator-mcp --pattern '*_linux_amd64.tar.gz'
gh attestation verify operator-mcp_*_linux_amd64.tar.gz --repo temporalio/operator-mcp
tar -xzf operator-mcp_*_linux_amd64.tar.gz operator-mcp
```

## Build from source

With Go 1.27.2 or later:

```sh
go build -o bin/ .
```

This writes `bin/operator-mcp`.

## HTTPS

With a `listen_tls` section, `operator-mcp` serves HTTPS on `listen`. Without one, it serves plain HTTP, for running behind a reverse proxy or load balancer that terminates TLS.

```yaml
listen_tls:
  cert_file: /etc/operator-mcp/tls.crt   # PEM certificate chain, leaf first
  key_file: /etc/operator-mcp/tls.key    # PEM private key
```

A renewed certificate takes effect for new connections without a restart.

## Auth

Each issuer's `audience` must identify this server as an API, such as its resource URL or API identifier, never the client app users sign in with. ID tokens issued to that app have the same `aud`, so they would pass as access tokens. With Microsoft Entra ID v2, the audience is the client ID of the API's app registration, which must be a different registration from the one the MCP client signs in with.

## Authorization

A config with an `auth` section must also have an `authorization` section, which decides which tools each caller may use on which targets:

```yaml
authorization:
  profiles:
    oncall:
      tools: [list_available_targets, check_connection, get_workflow]
      targets: [payments-prod, payments-staging, orders-prod]
    payments-dev:
      tools: [list_available_targets, get_workflow]
      targets: [payments-staging]
  rules:
    - {claim_name: groups, claim_value: temporal-oncall, profile: oncall}
    - {claim_name: roles, claim_value: Temporal.PaymentsDev, profile: payments-dev}
```

- A profile lets a caller use each of its `tools` on each of its `targets`. Targets are named by `name`, not by alias.
- A rule gives its `profile` to callers whose access token has the top-level claim `claim_name` as a string equal to `claim_value`, or as a list with `claim_value` as an entry; a `scope` or `scp` string counts as a list separated by spaces. A scope records what the client was granted, not who the user is, so prefer groups or roles. Matching is exact and case-sensitive. A missing claim, or a claim of another type, does not match.
- Copy rules from a decoded access token: `claim_name` is the claim's name exactly as it appears there, so names such as `cognito:groups` or `https://acme.example/roles` work as written, and `claim_value` is its value or one entry of its list.
- Match only claims that neither the user nor the client can set, such as groups or roles your identity provider assigns, and configure only issuers that issue tokens to your users alone. A rule on a claim the user or client can choose, such as `email`, lets them choose their own profile, and on an issuer that issues tokens to anyone, such as a CI provider's OIDC endpoint, it lets anyone get that profile.
- A rule with `issuer` matches only tokens from that issuer, which must be the `issuer` URL of one of the `auth.issuers` entries.
- A caller gets every profile whose rule it matches. Profiles combine per target: a profile that gives `get_workflow` on one target and another that gives `check_connection` on a second do not give `get_workflow` on the second.
- `tools/list` shows a caller only the tools it may use on some target. `list_available_targets` lists every target the caller may use some tool on.
- A caller that matches no rule may use no tool and sees no target. Its token is still accepted.

Without an `auth` section, an `authorization` section is not allowed, and every caller may use every tool on every target.

With Microsoft Entra ID, match [app roles](https://learn.microsoft.com/en-us/entra/identity-platform/howto-add-app-roles-in-apps) in the `roles` claim rather than groups: the `groups` claim holds group object IDs by default, not names, and is left out of tokens for users in more than 200 groups. Use the app role's Value as `claim_value`, not its Display name.

## Payloads

Payloads are your application's data in Temporal: workflow inputs and results, memos, heartbeat details and failure details. Each target's `payload_handling` says how tool responses show them:

- `omit`, the default, leaves payloads out. It hides only payloads: failure messages, stack traces, termination reasons and search attribute values are still shown.
- `passthrough` returns them as Temporal stores them, in base64, which the model can read unless your codec encrypts them.
- `decode` has the target's `codec` server decode them. If decoding fails, every payload in that response is left out, and the cause goes to the log.

```yaml
codecs:
  payments:
    endpoint: https://codec.payments.example.com
    authorization_header: {env: PAYMENTS_CODEC_AUTHORIZATION}
targets:
  - name: payments-prod
    # ...
    payload_handling: decode
    codec: payments
```

- `endpoint` is the same value as the CLI's `--codec-endpoint`, including its `{namespace}` placeholder.
- `authorization_header`, which is optional, holds the whole `Authorization` header value, such as `Bearer abc`, like `--codec-auth`. Its `env` is read at startup and its `file` on every request. The codec server must accept this header from `operator-mcp`, which cannot pass on a user's Temporal Cloud login as the Temporal UI does.

## Audit

`operator-mcp` writes an audit record of each tool call that passes the token check to stdout, as one line of JSON, and its logs to stderr.

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

The end-to-end tests in `internal/server` use the `temporal` CLI on your `PATH` to start a Temporal dev server. Without it, the first test run downloads the CLI.

CI runs the tests with the race detector, which does not cover the `operator-mcp` binary that the tests in `main_test.go` build and run:

```sh
go test -race ./...
```
