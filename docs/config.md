# Config reference

`temporal-mcp` reads its configuration from one YAML file, passed with `--config`, and rejects any key it does not know. In HTTP mode, the default, the server serves MCP over HTTP or HTTPS to many users and can authenticate them. In `--stdio` mode it serves the one agent (the AI application, an MCP client) that launches it, over stdin and stdout. [`examples/`](../examples) has a working config for each mode.

## Config file layout

```yaml
targets:
  - name: <string>
    [ aliases: [ <string>, ... ] ]
    description: <string>
    address: <host:port>
    namespace: <string>
    [ api_key: <secret> ]
    [ tls:
        cert_file: <filename>
        key_file: <filename> ]
    [ payload_handling: <string> | default = omit ]
    [ codec: <string> ]

[ codecs:
    <string>:
      endpoint: <url>
      [ allow_plain_http: <boolean> | default = false ]
      [ authorization_header: <secret> ] ]

[ audit_log_file: <filename> ]
[ error_log_file: <filename> ]
[ shutdown_timeout: <duration> | default = 8s ]

# HTTP mode only.
listen: <host:port>

# HTTP mode only.
[ listen_tls:
    cert_file: <filename>
    key_file: <filename> ]

# HTTP mode only.
[ auth:
    resource: <url>
    issuers:
      - issuer: <url>
        audience: <string>
    required_scopes: [ <string>, ... ]
    [ authorization_servers: [ <url>, ... ] | default = each issuer ]
    [ scopes_supported: [ <string>, ... ] | default = required_scopes ] ]

# HTTP mode only. Required with auth, not allowed without it.
[ authorization:
    profiles:
      <string>:
        tools: [ <string>, ... ]
        targets: [ <string>, ... ]
    rules:
      - claim_name: <string>
        claim_value: <string>
        [ issuer: <url> ]
        profile: <string> ]
```

A value in `< >` is a placeholder for a value of that type, defined under [Types](#types). A field in `[ ]` is optional, and `| default = x` is the value it takes when left out. When the brackets enclose a whole section, the section is optional, and the fields inside it without brackets are required when it is present. `[ <string>, ... ]` is a list, a list of maps shows one entry to repeat as needed, and a `<string>:` key is a name you choose. Sections without a comment are allowed in both modes.

## `targets`

Required in both modes, with at least one entry. Each target is one Temporal namespace that the server can read from. Agents name a target in each tool call, and the `list_available_targets` tool shows them each target's name, aliases, namespace, description and `payload_handling`.

- `name` (string, required): The name agents and authorization profiles use for the target. Names must be unique and are compared exactly, so `prod` and `Prod` are two different targets.
- `aliases` (list of strings, optional): Other names agents may use for the target. A target name in a tool call matches an alias ignoring case, but a `name` only exactly. An alias must not be empty, and must differ from every name and every other alias ignoring case, including its own target's name.
- `description` (string, required): What the target is, shown to agents so they can pick the right one.
- `address` (host:port, required): The namespace's gRPC endpoint, such as `my-namespace.a1b2c.tmprl.cloud:7233` or `localhost:7233`. Both the host and the port are required.
- `namespace` (string, required): The Temporal namespace, such as `my-namespace.a1b2c` or `default`.
- `api_key` (secret, optional): Where to read the API key the server connects with. `api_key:` with no value is an error.
- `tls` (optional): A client certificate the server presents to the target instead of an API key, as Temporal Cloud namespaces that use mTLS require. It is not allowed with `api_key`, and `tls:` with no value is an error.
  - `cert_file` (filename, required): Path of a PEM certificate chain, leaf first.
  - `key_file` (filename, required): Path of the certificate's PEM private key.
- `payload_handling` (string, default `omit`): How tool responses show the target's payloads, one of `omit`, `passthrough` or `decode`, described under [Payloads and `codecs`](#payloads-and-codecs).
- `codec` (string, required with `payload_handling: decode`): The name of the codec server in `codecs` that decodes the target's payloads. When set, it must be a key of `codecs`. It is ignored unless `payload_handling` is `decode`.

With an `api_key`, the server connects to the target over TLS and sends the key with each call, and with `tls` it connects over TLS and presents the certificate. With neither, it connects in plaintext with no credentials, as for a local dev server. Over TLS, the Temporal server's own certificate must chain to a [trusted CA](#trusted-cas), as Temporal Cloud's does. A target whose `address` host ends in `.tmprl.cloud` or `.api.temporal.io` and that has an `api_key` also connects to the Temporal Cloud Ops API at `saas-api.tmprl.cloud:443` with the same key, which `list_search_attributes` and `search_workflows` use. A Temporal Cloud target with `tls` does not connect to the Cloud Ops API, which takes only API keys, so `list_search_attributes` lists only the target's system search attributes, and `search_workflows` does not check that the names in its `fields` argument are search attributes on the target.

The server only reads, so give it a credential with read-only access to the namespace. On Temporal Cloud, a client certificate (`tls`) cannot be limited to Read, so if the namespace accepts API keys, use an [API key](https://docs.temporal.io/cloud/api-keys) of a [service account](https://docs.temporal.io/cloud/manage-access/service-accounts) created for the server: a [namespace-scoped](https://docs.temporal.io/cloud/manage-access/service-accounts#scoped) one with the [Read permission](https://docs.temporal.io/cloud/manage-access/permissions-reference#namespace-level-permissions), or one with the Read-Only account role and Read on the namespace. Read allows every call the server makes, including to the Cloud Ops API. Do not use the **Generate API Key** button on a namespace's page, which issues a key for the namespace's service account, an account with the Admin permission by default that other clients, such as workers, may also use. On Temporal Cloud, a namespace that accepts only client certificates has no read-only option until you turn on API keys for it.

With `tls`, the server does not start if it cannot load the certificate and key, and starts with a warning in the log if the certificate has expired. It reads them again for each new connection to the target, so a renewed certificate takes effect without a restart once the Temporal server replaces the connection.

## Payloads and `codecs`

Payloads are your application's data in Temporal: workflow inputs and results, memos, heartbeat details and failure details. Each target's `payload_handling` says how tool responses show them:

- `omit` leaves each payload's data out and shows only its encoding and its size in bytes as stored, so you can tell whether `passthrough` would show it as JSON or it needs `decode`. A `binary/null` payload, such as the result of a workflow that returned nothing, shows encoding `binary/null` and size zero. It hides only payloads, so failure messages, stack traces, termination reasons and search attribute values are still shown.
- `passthrough` shows payloads as Temporal stores them, which the agent can read unless your application encrypts them. A payload with encoding `json/plain` or `json/protobuf`, such as one from the SDKs' default data converter, is shown as JSON, and a `binary/null` payload as JSON `null`. Any other payload, including one whose encoding says JSON but whose data is not valid JSON, is shown as its encoding and its data in base64.
- `decode` has the target's codec server decode payloads, and shows a decoded payload the way `passthrough` shows a stored one, except that a decoded payload with a JSON encoding but data that is not valid JSON is left out as with `omit`, showing its encoding and size as stored. If the request to the codec server fails, every payload in that response is left out the same way. Either way, each payload left out has an `error` saying why, such as `could not connect to the codec server: connection refused`, and the full cause goes to the log. The `error` never includes the codec server's URL or response. For a target that decodes, `check_connection` also checks its codec server.

A codec server is an HTTP service that decodes payloads, the same one the Temporal UI and CLI use. `codecs` is optional in both modes, and maps a name of your choosing, which targets give in their `codec` field, to a codec server:

- `endpoint` (url, required): The codec server's URL without the `/decode` path, the same value as the CLI's `--codec-endpoint`. It must be an `https` URL, or an `http` one only for `localhost` or a loopback IP address (or for any host with `allow_plain_http: true`), with a host and no user info, query (`?`) or fragment (`#`). `{namespace}` in it is replaced with the target's `namespace`, and each request also carries the namespace in an `X-Namespace` header. The server does not follow redirects, so the endpoint must be the codec server's final URL.
- `allow_plain_http` (boolean, default `false`): Allows an `http` endpoint on any host. Over plain HTTP, the decoded payloads and the `Authorization` header travel unencrypted, so set it only when something else, such as a service mesh, encrypts the traffic. It has no effect on an `https` endpoint.
- `authorization_header` (secret, optional): Where to read the whole `Authorization` header value sent to the codec server, such as `Bearer abc`, like the CLI's `--codec-auth`. Without one, requests carry no `Authorization` header. The codec server must accept this value from `temporal-mcp`, which cannot pass on a user's Temporal Cloud login as the Temporal UI does. `authorization_header:` with no value is an error.

An `https` codec server's certificate must chain to a [trusted CA](#trusted-cas).

## `audit_log_file` and `error_log_file`

Optional in both modes. These send the server's audit records and logs to files.

- `audit_log_file` (filename, optional): The file the server appends its audit records to, one line of JSON for each tool call that passes the token check. Without it, the records go to stdout in HTTP mode and to stderr in `--stdio` mode.
- `error_log_file` (filename, optional): The file the server appends its logs to. Without it, the logs go to stderr in both modes. The `listening on` line and any error that stops the server still go to stderr.

The server creates each file with mode 0600 if it does not exist, and does not start if it cannot open one of them. It opens each file once, at startup, and never reopens it, so rotate the files with logrotate's `copytruncate` or restart the server after rotating them. Otherwise, after a rotation that renames a file, the server keeps writing to the renamed file, and its records are lost once that file is deleted.

## `listen` and `listen_tls`

These say where and how the server serves MCP in HTTP mode, and are not allowed in `--stdio` mode.

- `listen` (host:port, required in HTTP mode): The address the server binds to. MCP is served at the path `/mcp`. Without an `auth` section, the host must be a loopback IP address such as `127.0.0.1` or `::1`, because without auth anyone who can reach the port can use every tool on every target; a hostname such as `localhost`, an empty host such as `:8080`, and `0.0.0.0` are rejected. Requests must then also name a loopback host (`localhost`, `127.0.0.1` or `::1`) in their `Host` header, or they get a 403. With `auth`, any `host:port` is allowed, including `0.0.0.0:8443` or `:8443`.
- `listen_tls` (optional): With this section, the server serves HTTPS on `listen`. Without it, the server serves plain HTTP, for running behind a reverse proxy or load balancer that terminates TLS. `listen_tls:` with no value is an error.
  - `cert_file` (filename, required): Path of a PEM certificate chain, leaf first.
  - `key_file` (filename, required): Path of the certificate's PEM private key.

The server does not start if it cannot load the certificate and key, and starts with a warning in the log if the certificate has expired. It reads them again for each new connection, so a renewed certificate takes effect without a restart.

## `shutdown_timeout`

Optional in both modes, default `8s`; zero uses the default and negative values are rejected. After SIGINT or SIGTERM, or stdin closing in `--stdio` mode, the server waits this long for calls to finish, then cancels those still running and gives them another second to write their audit records. Set it at least a second shorter than the time your platform allows before SIGKILL. See [Shutdown](../README.md#shutdown).

## `auth`

Optional, HTTP mode only. With this section, every request to `/mcp` must carry a valid JWT access token from one of the `issuers`, and the server serves OAuth protected resource metadata, a JSON document that tells agents which identity provider to sign in with, at `/.well-known/oauth-protected-resource` followed by the path of `resource` (such as `/.well-known/oauth-protected-resource/mcp`). A config with `auth` must also have an [`authorization`](#authorization) section. `auth:` with no value is an error.

An agent gets its access token by signing the user in to an identity provider that the metadata names. The server verifies each token itself with the provider's signing keys, so the provider must issue access tokens as JWTs rather than opaque strings, signed with RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512 or EdDSA, with `exp` and `sub` claims, carrying this server's `audience` in their `aud` claim and each of `required_scopes` in their `scope` or `scp` claim.

When the agent signs the user in, it asks for the scopes in `scopes_supported`, which defaults to `required_scopes`, and sends the `resource` URL as the OAuth `resource` parameter. Register this server as an API with the provider, with `resource` as its identifier where the provider allows it, and give the API a scope that grants access to it, which goes in `required_scopes`. Some providers pick a token's `aud` from the requested scopes rather than from `resource`. Copy `audience` and `required_scopes` from a decoded access token.

The agent also needs a client ID the provider accepts, either a URL from which the provider reads the agent's details (a client ID metadata document) or one the agent registers for itself (dynamic client registration), if the provider and the agent both support it. Otherwise, register a public client (one with no secret) with the provider that uses PKCE, and have users enter its client ID in their agent. Its redirect URI is usually a loopback URL on a port the agent picks at random, so the provider must accept any port there.

- `resource` (url, required): The URL at which agents reach this server's MCP endpoint, such as `https://temporal-mcp.example.com/mcp`.
- `issuers` (list, required): The identity providers whose access tokens the server accepts, at least one.
  - `issuer` (url, required): The exact `iss` claim of the provider's tokens, including any trailing slash. Each must be unique. When the server first verifies a token from an issuer, it fetches the `issuer` URL, without any trailing slash, followed by `/.well-known/openid-configuration`, and the `issuer` in that document must equal this value exactly.
  - `audience` (string, required): A value the `aud` claim of a token for this server must contain. It must identify this server as an API, such as its resource URL or API identifier, and never the client app users sign in with, because ID tokens issued to that app carry the same `aud` and would pass as access tokens. Copy it from the `aud` claim of a decoded access token for this server.
- `required_scopes` (list of strings, required): The scopes every access token must carry, at least one. A token carries a scope when it is an entry of the token's `scope` or `scp` claim, where a string claim counts as a list of its space-separated entries, and matching is exact and case-sensitive. A valid token missing any of them gets a 403 with `error="insufficient_scope"` in the response's `WWW-Authenticate` challenge, before any [`authorization`](#authorization) rule is checked. Use the scope you gave this server's API, as it appears in the `scope` or `scp` claim of a decoded access token for this server, never one every token carries, such as `openid`.
- `authorization_servers` (list of urls, default each `issuer`): Where agents are told to get tokens. Set it only if that differs from the `issuer` URLs.
- `scopes_supported` (list of strings, default `required_scopes`): The scopes agents request, advertised in the metadata and in the `WWW-Authenticate` challenge in place of `required_scopes`. Set it only when agents must request a scope by a different name than the one tokens carry, as in Microsoft Entra ID: agents request `api://<app-id>/Temporal.Read`, which goes here, and the token's `scp` claim holds `Temporal.Read`, which goes in `required_scopes`. The server does not use these to grant or deny access, but with some providers they decide the token's `aud`, as described above.

`resource`, each `issuer` and each `authorization_servers` entry must be an `https` URL, or an `http` one only for `localhost` or a loopback IP address, with a host and no user info, query or fragment. Each `required_scopes` and `scopes_supported` entry must be non-empty and contain only printable ASCII characters other than space, `"` and `\`.

## `authorization`

Decides which tools each caller may use on which targets. It is HTTP mode only, and is required with `auth` and not allowed without it, because its rules match claims in the access tokens that `auth` verifies. Without `auth`, every caller may use every tool on every target. `authorization:` with no value is an error. [`examples/http-auth-config.yaml`](../examples/http-auth-config.yaml) shows a complete section.

- `profiles` (map from a profile name to a profile, required): Each profile lets a caller use each of its `tools` on each of its `targets`.
  - `tools` (list of strings, required): At least one of `list_available_targets`, `check_connection`, `get_workflows`, `get_workflow_history`, `get_task_queues`, `search_workflows`, `count_workflows` and `list_search_attributes`. Any other name is an error.
  - `targets` (list of strings, required): At least one target, each given by its `name`, not an alias.
- `rules` (list, required): Which callers get which profile, at least one.
  - `claim_name` (string, required): The name of a top-level claim exactly as it appears in a decoded access token, so names such as `cognito:groups` or `https://example.com/roles` work as written. It must not have leading or trailing whitespace.
  - `claim_value` (string, required): The value the claim must equal, or one entry of it when the claim is a list. It must be a YAML string, so an unquoted number or boolean such as `42` or `true` is rejected; quote it (`"42"`) to match the string. A `scope` or `scp` claim that is a string counts as a list of its space-separated entries, so a value matching one of them must not contain a space, and each scope needs its own rule.
  - `issuer` (url, optional): Limits the rule to tokens from this issuer, which must be the `issuer` of an `auth.issuers` entry. When `auth` has more than one issuer, the server logs a warning at startup for each rule without one, since such a rule matches tokens from every issuer.
  - `profile` (string, required): The name of a profile in `profiles`.

A rule matches a caller whose access token has the claim `claim_name` as a string equal to `claim_value`, or as a list with `claim_value` as an entry. Matching is exact and case-sensitive, and a missing claim or a claim of another type does not match. A caller gets the profile of every rule it matches, and profiles combine per target: a profile that gives `get_workflows` on one target and another that gives `check_connection` on a second do not give `get_workflows` on the second. Agents see only the tools the caller may use on some target, and `list_available_targets` lists every target the caller may use some tool on. A caller that matches no rule may use no tool and sees no target, although its token is still accepted.

Rules should match only claims that neither the user nor the client can set, such as the groups or roles your identity provider assigns, from issuers that issue tokens only to your users. A rule on a claim the user or client can choose, such as `email`, lets them choose their own profile, and a rule that accepts an issuer that issues tokens to anyone, such as a CI provider's OIDC endpoint, lets anyone get that profile. A scope records what the client was granted rather than who the user is, so prefer groups or roles to scopes. Copy `claim_name` and `claim_value` from a decoded access token of a real user rather than from the identity provider's console, which may show display names or leave out claims the token does not carry.

## `--stdio` mode

In `--stdio` mode the config has `targets` and, optionally, `codecs`, `audit_log_file`, `error_log_file` and `shutdown_timeout`; `listen`, `listen_tls`, `auth` and `authorization` are not allowed. Since stdout carries MCP messages, audit records go to stderr unless `audit_log_file` is set. There is no authorization: the targets' API keys and client certificates decide what the developer can read. The agent picks the directory it starts the server in, so give the `--config` path and every file path in the config as absolute paths. The server sees only the environment the MCP client passes it, so set `SSL_CERT_FILE` and any variable an `env` secret names as literal values in the client's config, or use `file` secrets.

## Trusted CAs

The server checks that the certificate of everything it connects to over TLS, whether a target, a codec server, the Temporal Cloud Ops API or an identity provider, chains to a CA (certificate authority) the machine trusts. To add a private CA, such as one that signs a self-hosted Temporal server's certificate, set the `SSL_CERT_FILE` or `SSL_CERT_DIR` environment variable. Either one can replace the machine's trusted CAs for all of those connections, so it must include the public CAs too. A path to a file that does not exist can leave the server trusting no CA, with no error at startup. On macOS, adding the CA to the keychain and marking it trusted also works and keeps the public CAs, but only while neither variable is set.

## Types

- `<string>`: A YAML string.
- `<boolean>`: A YAML boolean, `true` or `false`.
- `<host:port>`: A host and port, such as `127.0.0.1:8080` or `my-namespace.a1b2c.tmprl.cloud:7233`.
- `<url>`: An absolute URL. Each field that takes one says which schemes it allows.
- `<filename>`: The path of a file on the machine the server runs on. A relative path resolves against the directory the server is started in, not the config file's directory.
- `<secret>`: Where to read a secret, never the secret itself. It is a map with exactly one of these keys, and either form is trimmed of surrounding whitespace:
  - `env` (string): The name of an environment variable holding the secret. The server reads it at startup and on every use, and does not start if it is unset or empty.
  - `file` (filename): The path of a file holding only the secret itself, such as the API key. The server reads it at startup, and does not start if the file cannot be read or is empty. It reads the file again on every use, so a rotated secret takes effect without a restart, and a use fails if the file cannot be read or is empty, with the cause in the log.

  A secret field with no value, such as `authorization_header:` on its own line, is an error.
