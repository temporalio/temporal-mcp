# Config reference

`operator-mcp` reads one YAML file, passed with `--config`. For working configs to start from, see [`examples/`](../examples).

The server runs in one of two modes: **HTTP mode**, the default, serves MCP over HTTP or HTTPS to many users; **`--stdio` mode** serves the one MCP client that launches it, over stdin and stdout. The config's sections depend on the mode:

| Section | HTTP mode (default) | `--stdio` mode |
| --- | --- | --- |
| [`listen`](#listen) | Required | Not allowed |
| [`listen_tls`](#listen_tls) | Optional | Not allowed |
| [`targets`](#targets) | Required | Required |
| [`codecs`](#codecs) | Optional | Optional |
| [`auth`](#auth) | Optional | Not allowed |
| [`authorization`](#authorization) | Required with `auth`, not allowed without it | Not allowed |

General rules:

- A key the server does not know, at any level, is an error, so a misspelled key fails instead of being ignored.
- `listen_tls:`, `auth:` or `authorization:` with no value counts as an empty section and fails validation; `codecs:`, `api_key:` or `authorization_header:` with no value is the same as leaving the key out.
- The server reports the validation problems it finds and does not start. An unknown key or a value of the wrong type is reported first, and the checks at startup, such as environment variables being set, run only after validation passes, so fixing one round of errors can reveal more.

## Secrets

`api_key` and `authorization_header` say where to read a secret, never the secret itself. Each takes exactly one of:

| Field | Type | Meaning |
| --- | --- | --- |
| `env` | string | Name of an environment variable holding the secret. It is read once, at startup, and the server does not start if it is unset or empty. |
| `file` | string | Path of a file holding the secret. It is read on every use, so a rotated secret takes effect without a restart. A use fails if the file cannot be read or is empty. |

Both forms are trimmed of surrounding whitespace. Setting both, or neither, is an error.

## `listen`

HTTP mode only. Type: string. Required.

The `host:port` the HTTP server binds to. MCP is served at the path `/mcp`.

- Without an `auth` section, the host must be a loopback IP address, such as `127.0.0.1` or `::1`. A hostname such as `localhost`, an empty host such as `:8080`, and `0.0.0.0` are rejected, because without auth anyone who can reach the port can use every tool on every target. Requests must also name a loopback host (`localhost`, `127.0.0.1` or `::1`) in their `Host` header, or they get a 403.
- With an `auth` section, any `host:port` is allowed, including `0.0.0.0:8443` or `:8443`.

## `listen_tls`

HTTP mode only. Optional.

With this section, the server serves HTTPS on `listen`. Without it, the server serves plain HTTP, for running behind a reverse proxy or load balancer that terminates TLS.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `cert_file` | string | Yes | Path of a PEM certificate chain, leaf first. |
| `key_file` | string | Yes | Path of the certificate's PEM private key. |

The server does not start if the certificate and key cannot be loaded. It reads them again for each new connection, so a renewed certificate takes effect without a restart.

```yaml
listen_tls:
  cert_file: /etc/operator-mcp/tls.crt
  key_file: /etc/operator-mcp/tls.key
```

## `targets`

Both modes. Type: list. Required, with at least one entry.

Each target is one Temporal namespace that the server can read from. Agents name a target in each tool call, and `list_available_targets` shows them each target's name, aliases, namespace and description.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | Yes | The name agents and `authorization` profiles use for the target. |
| `aliases` | list of strings | No | Other names agents may use for the target. |
| `description` | string | Yes | What the target is, shown to agents so they can pick the right one. |
| `address` | string | Yes | `host:port` of the namespace's gRPC endpoint, such as `my-namespace.a1b2c.tmprl.cloud:7233` or `localhost:7233`. |
| `namespace` | string | Yes | The Temporal namespace, such as `my-namespace.a1b2c` or `default`. |
| `api_key` | [secret](#secrets) | No | Where to read the API key the server connects with. |
| `payload_handling` | `omit`, `passthrough` or `decode` | No (default: `omit`) | How tool responses show the target's payloads. See [Payloads](#payloads). |
| `codec` | string | With `payload_handling: decode` | The name of the entry in [`codecs`](#codecs) that decodes the target's payloads. |

Rules:

- Names must be unique. They are compared exactly, so `prod` and `Prod` are two different names.
- Aliases must not be empty, and must differ from every name and every other alias, including the target's own name, ignoring case. An agent's target name matches a `name` exactly or an alias ignoring case.
- `codec`, when set, must be a key of `codecs`.
- `codec` is ignored unless `payload_handling` is `decode`.

### `api_key`

With an `api_key`, the server connects to the target over TLS and sends the key with each call. Without one, it connects in plaintext with no credentials, as for a local dev server.

A target whose `address` ends in `.tmprl.cloud` or `.api.temporal.io` and that has an `api_key` also connects to the Temporal Cloud Ops API at `saas-api.tmprl.cloud:443`, with the same key, for `list_search_attributes` and `search_workflows`.

### Payloads

Payloads are your application's data in Temporal: workflow inputs and results, memos, heartbeat details and failure details. Each target's `payload_handling` says how tool responses show them:

- `omit`, the default, leaves payloads out. It hides only payloads: failure messages, stack traces, termination reasons and search attribute values are still shown.
- `passthrough` returns them as Temporal stores them, in base64, which the agent can read unless your application encrypts them.
- `decode` has the target's `codec` server decode them. If the codec request fails, every payload in that response is left out. A payload the codec server returns as JSON that is not valid JSON is left out on its own. Either way, the cause goes to the log. `check_connection` also checks the codec server of a target that decodes.

## `codecs`

Both modes. Type: map from a codec name to a codec. Optional.

Each entry is a codec server, which decodes payloads over HTTP, as for the Temporal UI and CLI. Targets name it in their `codec` field.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `endpoint` | string | Yes | The codec server's URL without the `/decode` path, the same value as the CLI's `--codec-endpoint`. |
| `authorization_header` | [secret](#secrets) | No | Where to read the whole `Authorization` header value sent to the codec server, such as `Bearer abc`, like the CLI's `--codec-auth`. Without one, requests carry no `Authorization` header. |

Rules:

- `endpoint` must be an `http` or `https` URL with a host and no user info, query (`?`) or fragment (`#`).
- `{namespace}` in `endpoint` is replaced with the target's `namespace`. Each request also carries the namespace in an `X-Namespace` header.
- The codec server must accept `authorization_header` from `operator-mcp`, which cannot pass on a user's Temporal Cloud login as the Temporal UI does.

```yaml
codecs:
  payments:
    endpoint: https://codec.example.com/{namespace}
    authorization_header: {env: PAYMENTS_CODEC_AUTHORIZATION}
targets:
  - name: payments-prod
    # ...
    payload_handling: decode
    codec: payments
```

## `auth`

HTTP mode only. Optional.

With this section, every request to `/mcp` must carry a valid JWT access token from one of the `issuers`, and the server serves OAuth protected resource metadata, a JSON document that tells MCP clients which identity provider to sign in with. A config with `auth` must also have an [`authorization`](#authorization) section.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `resource` | string | Yes | The URL at which clients reach this server's MCP endpoint, such as `https://temporal-mcp.example.com/mcp`. |
| `issuers` | list | Yes | The identity providers whose access tokens the server accepts; at least one. |
| `authorization_servers` | list of strings | No (default: each issuer) | Where clients are told to get tokens. Set it only if that differs from the `issuer` URLs. |
| `scopes_supported` | list of strings | No | The scopes advertised to clients, in the metadata and in the `WWW-Authenticate` challenge. They do not grant or deny access. |

Each entry in `issuers`:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `issuer` | string | Yes | The exact `iss` claim of the provider's tokens, including any trailing slash. |
| `audience` | string | Yes | A value the `aud` claim of a token for this server must contain. |

Rules:

- `resource`, each `issuer` and each `authorization_servers` entry must be an `https` URL, or an `http` one only for `localhost` or a loopback IP address, with a host and no user info, query or fragment.
- The metadata is served at `/.well-known/oauth-protected-resource` followed by the path of `resource`, such as `/.well-known/oauth-protected-resource/mcp`.
- Each `issuer` must be unique. The server fetches the `issuer` URL, without any trailing slash, followed by `/.well-known/openid-configuration`, when it first verifies a token from that issuer, and the `issuer` in that document must equal the configured one exactly.
- `audience` must not be empty. It must identify this server as an API, such as its resource URL or API identifier, never the client app users sign in with: ID tokens issued to that app have the same `aud`, so they would pass as access tokens.
- Each `scopes_supported` entry must be non-empty and contain only printable ASCII characters other than space, `"` and `\`.

```yaml
auth:
  resource: https://temporal-mcp.example.com/mcp
  issuers:
    - issuer: https://login.example.com/
      audience: https://temporal-mcp.example.com/mcp
```

## `authorization`

HTTP mode only. Required with `auth` and not allowed without it, because its rules match claims in the access tokens that `auth` verifies. Without it, every caller may use every tool on every target. With `--stdio`, the targets' API keys decide what the developer can read.

`authorization` decides which tools each caller may use on which targets:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `profiles` | map from a profile name to a profile | Yes | Sets of tools and targets. |
| `rules` | list | Yes | Which callers get which profile; at least one. |

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

### Profiles

A profile lets a caller use each of its `tools` on each of its `targets`.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `tools` | list of strings | Yes | At least one tool name: `list_available_targets`, `check_connection`, `get_workflow`, `get_workflow_history`, `get_task_queue`, `search_workflows`, `count_workflows` and `list_search_attributes`. The server does not start if a profile names any other tool. |
| `targets` | list of strings | Yes | At least one target name. Each must be the `name` of a target, not an alias. |

### Rules

A rule gives its `profile` to callers whose access token has the top-level claim `claim_name` as a string equal to `claim_value`, or as a list with `claim_value` as an entry.

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `claim_name` | string | Yes | The claim's name exactly as it appears in a decoded access token, so names such as `cognito:groups` or `https://example.com/roles` work as written. |
| `claim_value` | string | Yes | The claim's value, or one entry of its list. |
| `issuer` | string | No | Limits the rule to tokens from this issuer. It must be the `issuer` of one of the `auth.issuers` entries. |
| `profile` | string | Yes | The name of a profile in `profiles`. |

Rules:

- Matching is exact and case-sensitive. A missing claim, or a claim of another type, does not match.
- `claim_name` must not have leading or trailing whitespace.
- `claim_value` must be a YAML string. An unquoted number or boolean, such as `42` or `true`, is rejected; quote it (`"42"`) to match the string.
- A `scope` or `scp` claim that is a string counts as a list of its space-separated entries, so a `claim_value` for one of them must not contain a space. Write one scope per rule. A scope records what the client was granted, not who the user is, so prefer groups or roles.
- When `auth` has more than one issuer, the server logs a warning at startup for each rule without an `issuer`, which matches tokens from every issuer.
- Match only claims that neither the user nor the client can set, such as groups or roles your identity provider assigns, and configure only issuers that issue tokens to your users alone. A rule on a claim the user or client can choose, such as `email`, lets them choose their own profile, and on an issuer that issues tokens to anyone, such as a CI provider's OIDC endpoint, it lets anyone get that profile.
- Copy `claim_name` and `claim_value` from a decoded access token of a real user, not from the identity provider's console, which may show display names or leave out claims the token doesn't carry.

### What a caller gets

- A caller gets every profile whose rule it matches. Profiles combine per target: a profile that gives `get_workflow` on one target and another that gives `check_connection` on a second do not give `get_workflow` on the second.
- Agents see only the tools the caller may use on some target. `list_available_targets` lists every target the caller may use some tool on.
- A caller that matches no rule may use no tool and sees no target. Its token is still accepted.
