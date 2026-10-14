# Config reference

`operator-mcp` reads its configuration from one YAML file, passed with `--config`, and rejects any key it does not know. In HTTP mode, the default, the server serves MCP over HTTP or HTTPS to many users and can authenticate them. In `--stdio` mode it serves the one agent (the AI application, an MCP client) that launches it, over stdin and stdout. [`examples/`](../examples) has a working config for each mode.

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
      [ authorization_header: <secret> ] ]

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
    [ authorization_servers: [ <url>, ... ] | default = each issuer ]
    [ scopes_supported: [ <string>, ... ] ] ]

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
- `address` (host:port, required): The namespace's gRPC endpoint, such as `my-namespace.a1b2c.tmprl.cloud:7233` or `localhost:7233`.
- `namespace` (string, required): The Temporal namespace, such as `my-namespace.a1b2c` or `default`.
- `api_key` (secret, optional): Where to read the API key the server connects with. `api_key:` with no value is an error.
- `tls` (optional): A client certificate the server presents to the target instead of an API key, as Temporal Cloud namespaces that use mTLS require. It is not allowed with `api_key`, and `tls:` with no value is an error.
  - `cert_file` (filename, required): Path of a PEM certificate chain, leaf first.
  - `key_file` (filename, required): Path of the certificate's PEM private key.
- `payload_handling` (string, default `omit`): How tool responses show the target's payloads, one of `omit`, `passthrough` or `decode`, described under [Payloads and `codecs`](#payloads-and-codecs).
- `codec` (string, required with `payload_handling: decode`): The name of the codec server in `codecs` that decodes the target's payloads. When set, it must be a key of `codecs`. It is ignored unless `payload_handling` is `decode`.

With an `api_key`, the server connects to the target over TLS and sends the key with each call, and with `tls` it connects over TLS and presents the certificate. With neither, it connects in plaintext with no credentials, as for a local dev server. A target whose `address` host ends in `.tmprl.cloud` or `.api.temporal.io` and that has an `api_key` also connects to the Temporal Cloud Ops API at `saas-api.tmprl.cloud:443` with the same key, which `list_search_attributes` and `search_workflows` use. A Temporal Cloud target with `tls` does not connect to the Cloud Ops API, which takes only API keys, so `list_search_attributes` lists only the target's system search attributes, and `search_workflows` does not check that the names in its `fields` argument are search attributes on the target.

With `tls`, the server does not start if it cannot load the certificate and key, and starts with a warning in the log if the certificate has expired. It reads them again for each new connection to the target, so a renewed certificate takes effect without a restart once the Temporal server replaces the connection. The Temporal server's own certificate must chain to a CA in the system trust store, as Temporal Cloud's does.

## Payloads and `codecs`

Payloads are your application's data in Temporal: workflow inputs and results, memos, heartbeat details and failure details. Each target's `payload_handling` says how tool responses show them:

- `omit` leaves each payload's data out and shows only its encoding and its size in bytes as stored, so you can tell whether `passthrough` would show it as JSON or it needs `decode`. A `binary/null` payload, such as the result of a workflow that returned nothing, shows encoding `binary/null` and size zero. It hides only payloads, so failure messages, stack traces, termination reasons and search attribute values are still shown.
- `passthrough` shows payloads as Temporal stores them, which the agent can read unless your application encrypts them. A payload with encoding `json/plain` or `json/protobuf`, such as one from the SDKs' default data converter, is shown as JSON, and a `binary/null` payload as JSON `null`. Any other payload, including one whose encoding says JSON but whose data is not valid JSON, is shown as its encoding and its data in base64.
- `decode` has the target's codec server decode payloads, and shows a decoded payload the way `passthrough` shows a stored one, except that a decoded payload with a JSON encoding but data that is not valid JSON is left out as with `omit`, showing its encoding and size as stored. If the request to the codec server fails, every payload in that response is left out the same way. Either way, the cause goes to the log. For a target that decodes, `check_connection` also checks its codec server.

A codec server is an HTTP service that decodes payloads, the same one the Temporal UI and CLI use. `codecs` is optional in both modes, and maps a name of your choosing, which targets give in their `codec` field, to a codec server:

- `endpoint` (url, required): The codec server's URL without the `/decode` path, the same value as the CLI's `--codec-endpoint`. It must be an `http` or `https` URL with a host and no user info, query (`?`) or fragment (`#`). `{namespace}` in it is replaced with the target's `namespace`, and each request also carries the namespace in an `X-Namespace` header.
- `authorization_header` (secret, optional): Where to read the whole `Authorization` header value sent to the codec server, such as `Bearer abc`, like the CLI's `--codec-auth`. Without one, requests carry no `Authorization` header. The codec server must accept this value from `operator-mcp`, which cannot pass on a user's Temporal Cloud login as the Temporal UI does.

## `listen` and `listen_tls`

These say where and how the server serves MCP in HTTP mode, and are not allowed in `--stdio` mode.

- `listen` (host:port, required in HTTP mode): The address the server binds to. MCP is served at the path `/mcp`. Without an `auth` section, the host must be a loopback IP address such as `127.0.0.1` or `::1`, because without auth anyone who can reach the port can use every tool on every target; a hostname such as `localhost`, an empty host such as `:8080`, and `0.0.0.0` are rejected. Requests must then also name a loopback host (`localhost`, `127.0.0.1` or `::1`) in their `Host` header, or they get a 403. With `auth`, any `host:port` is allowed, including `0.0.0.0:8443` or `:8443`.
- `listen_tls` (optional): With this section, the server serves HTTPS on `listen`. Without it, the server serves plain HTTP, for running behind a reverse proxy or load balancer that terminates TLS. `listen_tls:` with no value is an error.
  - `cert_file` (filename, required): Path of a PEM certificate chain, leaf first.
  - `key_file` (filename, required): Path of the certificate's PEM private key.

The server does not start if it cannot load the certificate and key, and starts with a warning in the log if the certificate has expired. It reads them again for each new connection, so a renewed certificate takes effect without a restart.

## `auth`

Optional, HTTP mode only. With this section, every request to `/mcp` must carry a valid JWT access token from one of the `issuers`, and the server serves OAuth protected resource metadata, a JSON document that tells agents which identity provider to sign in with, at `/.well-known/oauth-protected-resource` followed by the path of `resource` (such as `/.well-known/oauth-protected-resource/mcp`). A config with `auth` must also have an [`authorization`](#authorization) section. `auth:` with no value is an error.

- `resource` (url, required): The URL at which agents reach this server's MCP endpoint, such as `https://temporal-mcp.example.com/mcp`.
- `issuers` (list, required): The identity providers whose access tokens the server accepts, at least one.
  - `issuer` (url, required): The exact `iss` claim of the provider's tokens, including any trailing slash. Each must be unique. When the server first verifies a token from an issuer, it fetches the `issuer` URL, without any trailing slash, followed by `/.well-known/openid-configuration`, and the `issuer` in that document must equal this value exactly.
  - `audience` (string, required): A value the `aud` claim of a token for this server must contain. It must identify this server as an API, such as its resource URL or API identifier, and never the client app users sign in with, because ID tokens issued to that app carry the same `aud` and would pass as access tokens. Copy it from the `aud` claim of a decoded access token for this server.
- `authorization_servers` (list of urls, default each `issuer`): Where agents are told to get tokens. Set it only if that differs from the `issuer` URLs.
- `scopes_supported` (list of strings, optional): The scopes advertised to agents, in the metadata and in the `WWW-Authenticate` challenge. They do not grant or deny access. Each must be non-empty and contain only printable ASCII characters other than space, `"` and `\`.

`resource`, each `issuer` and each `authorization_servers` entry must be an `https` URL, or an `http` one only for `localhost` or a loopback IP address, with a host and no user info, query or fragment.

## `authorization`

Decides which tools each caller may use on which targets. It is HTTP mode only, and is required with `auth` and not allowed without it, because its rules match claims in the access tokens that `auth` verifies. Without `auth`, every caller may use every tool on every target. `authorization:` with no value is an error. [`examples/http-auth.yaml`](../examples/http-auth.yaml) shows a complete section.

- `profiles` (map from a profile name to a profile, required): Each profile lets a caller use each of its `tools` on each of its `targets`.
  - `tools` (list of strings, required): At least one of `list_available_targets`, `check_connection`, `get_workflow`, `get_workflow_history`, `get_task_queue`, `search_workflows`, `count_workflows` and `list_search_attributes`. Any other name is an error.
  - `targets` (list of strings, required): At least one target, each given by its `name`, not an alias.
- `rules` (list, required): Which callers get which profile, at least one.
  - `claim_name` (string, required): The name of a top-level claim exactly as it appears in a decoded access token, so names such as `cognito:groups` or `https://example.com/roles` work as written. It must not have leading or trailing whitespace.
  - `claim_value` (string, required): The value the claim must equal, or one entry of it when the claim is a list. It must be a YAML string, so an unquoted number or boolean such as `42` or `true` is rejected; quote it (`"42"`) to match the string. A `scope` or `scp` claim that is a string counts as a list of its space-separated entries, so a value matching one of them must not contain a space, and each scope needs its own rule.
  - `issuer` (url, optional): Limits the rule to tokens from this issuer, which must be the `issuer` of an `auth.issuers` entry. When `auth` has more than one issuer, the server logs a warning at startup for each rule without one, since such a rule matches tokens from every issuer.
  - `profile` (string, required): The name of a profile in `profiles`.

A rule matches a caller whose access token has the claim `claim_name` as a string equal to `claim_value`, or as a list with `claim_value` as an entry. Matching is exact and case-sensitive, and a missing claim or a claim of another type does not match. A caller gets the profile of every rule it matches, and profiles combine per target: a profile that gives `get_workflow` on one target and another that gives `check_connection` on a second do not give `get_workflow` on the second. Agents see only the tools the caller may use on some target, and `list_available_targets` lists every target the caller may use some tool on. A caller that matches no rule may use no tool and sees no target, although its token is still accepted.

Rules should match only claims that neither the user nor the client can set, such as the groups or roles your identity provider assigns, from issuers that issue tokens only to your users. A rule on a claim the user or client can choose, such as `email`, lets them choose their own profile, and a rule that accepts an issuer that issues tokens to anyone, such as a CI provider's OIDC endpoint, lets anyone get that profile. A scope records what the client was granted rather than who the user is, so prefer groups or roles to scopes. Copy `claim_name` and `claim_value` from a decoded access token of a real user rather than from the identity provider's console, which may show display names or leave out claims the token does not carry.

## `--stdio` mode

In `--stdio` mode the config has only `targets` and, optionally, `codecs`; `listen`, `listen_tls`, `auth` and `authorization` are not allowed. There is no authorization: the targets' API keys and client certificates decide what the developer can read.

## Types

- `<string>`: A YAML string.
- `<host:port>`: A host and port, such as `127.0.0.1:8080` or `my-namespace.a1b2c.tmprl.cloud:7233`.
- `<url>`: An absolute URL. Each field that takes one says which schemes it allows.
- `<filename>`: The path of a file on the machine the server runs on.
- `<secret>`: Where to read a secret, never the secret itself. It is a map with exactly one of these keys, and either form is trimmed of surrounding whitespace:
  - `env` (string): The name of an environment variable holding the secret. The server reads it once, at startup, and does not start if it is unset or empty.
  - `file` (filename): The path of a file holding the secret. The server reads it on every use, so a rotated secret takes effect without a restart, and a use fails if the file cannot be read or is empty.

  A secret field with no value, such as `authorization_header:` on its own line, is the same as leaving the field out, except `api_key`, where it is an error.
