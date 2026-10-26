# Identity and access

This guide describes the identity contract for a centrally hosted
Temporal MCP deployment. It is intentionally provider-neutral: an
organization may use Entra ID, Okta, or another OpenID Connect provider, while
the server needs the same small set of token properties from each one.

For server placement and target credentials, see [Deployment](deployment.md).
For a complete configuration, see the
[centrally hosted OIDC and mTLS example](central-oidc-mtls.example.md).

## Components

- **User:** the person asking an AI application to investigate Temporal.
- **MCP client:** the AI application that connects to the MCP endpoint and
  obtains a token for the user.
- **Identity provider:** the authorization server that authenticates the user
  and issues a signed access token.
- **MCP resource server:** Temporal MCP, which validates the token and
  maps its claims to tools and targets.
- **Temporal target:** a configured namespace that the MCP server reaches with
  its own API key or mTLS certificate.

The user's access token is not forwarded to Temporal. Likewise, the Temporal
credential is never sent to the MCP client.

## Requirements

For each trusted identity provider, the server needs:

- An **issuer** equal to the access token's `iss` claim. The value must match
  exactly, including a trailing slash when the issuer uses one.
- An **audience** contained in the access token's `aud` claim. It must identify
  the MCP API, not the client application in which the user signs in.
- OIDC discovery and signing-key endpoints reachable from the server.
- A signed JWT **access token**. An ID token describes a sign-in to the client
  and must not be used as the API bearer token.
- An existing group-membership claim configured to appear in access tokens for
  the MCP API.

The MCP server is an OAuth resource server. It validates tokens and publishes
protected-resource metadata, but it does not create users, assign groups, or
act as the organization's identity provider.

## Registration

Identity providers commonly distinguish between an API resource and a client
application:

1. The **API resource** represents Temporal MCP. It defines the
   audience used in access tokens and, where the provider requires them,
   scopes.
2. The **client application** represents the MCP client that signs the user in.
   It has the redirect URI and OAuth flow required by that client and requests
   a token for the MCP API resource.

The exact objects and names vary by provider and MCP client. Some organizations
may already have a suitable client registration, while others must create one.
The durable requirement is that the MCP client can obtain an access token whose
issuer, audience, and authorization claims satisfy the MCP server config.

**In the identity provider:** expose a delegated scope for the MCP API, allow
the MCP client to request it, grant any required consent, and include the chosen
existing group in the access token.

**In the Temporal MCP config:** enter the provider's issuer, the MCP API
audience, and the same scope. Then map the group claim to a profile. The scope
lets the client obtain a token for Temporal MCP; the profile controls its tools
and Temporal targets.

The Temporal Cloud Web UI is a separate relying party. Its SSO configuration does not
provide an access token for Temporal MCP. The identity provider must issue a
token whose audience identifies the MCP API.

## Configuration

The identity provider and MCP configuration answer different questions:

| Question | Where it is answered |
| --- | --- |
| Is this token authentic and intended for the MCP API? | `auth.issuers` validates issuer, signature, time, and audience |
| Which users belong to a group? | Identity-provider group membership |
| What may that group do in MCP? | `authorization.profiles` lists tools and targets |
| Which token claim grants which MCP profile? | `authorization.rules` maps a claim value to a profile |
| What may the server do in Temporal? | The API key or mTLS identity configured on each target |

A profile is a named set of tool and target permissions. A rule grants that
profile when a token contains a matching claim. The profile name therefore
appears once where its permissions are defined and again where the rule refers
to it. More than one rule can grant the same profile.

For example:

```yaml
auth:
  resource: https://temporal-mcp.example.com/mcp # Example MCP URL.
  issuers:
    - issuer: https://id.example.com/ # Example identity provider.
      audience: api://temporal-mcp # Example API audience.
  scopes_supported:
    - temporal-mcp.access # Example scope configured in the identity provider.

authorization:
  profiles:
    mcp-user: # Example profile name.
      tools: [list_available_targets, check_connection, get_workflows]
      targets: [payments-production] # Example target name.
  rules:
    - claim_name: groups # Example group claim in the access token.
      claim_value: engineering # Example existing group name or ID.
      profile: mcp-user # Refers to the profile above.
```

In this example, members of the existing `engineering` group receive the
`mcp-user` profile. The MCP configuration defines which tools and Temporal
targets that profile can use. No Temporal-specific role is required in the
identity provider. Use the group value exactly as it appears in the access
token; depending on the provider, that may be a name or an ID.

## Interpreting Failures

- **No token or an invalid token:** the request receives 401 Unauthorized.
- **Valid token, no matching rule:** authentication succeeds, but the caller
  sees no tools or targets.
- **Matching group, missing tool:** the caller does not see or cannot invoke that
  tool.
- **Matching group and tool, missing target:** the tool cannot be used on that
  target.
- **MCP authorization succeeds, Temporal denies the call:** the target
  credential lacks the required backend permission.

This fail-closed behavior lets identity, MCP policy, and Temporal permissions
be tested separately.

## Appendix

### Claims

Prefer claims that users and client applications cannot set for themselves.
Centrally managed groups are a good default. Avoid using email addresses or
other self-editable profile data as authorization policy.

Use a real decoded access token to choose `claim_name` and `claim_value`:

- Provider consoles may display a friendly group name while the token contains
  an opaque group identifier.
- Large group memberships may be omitted or represented through an overage
  mechanism rather than a normal claim.
- Claim names and values are case-sensitive in MCP authorization rules.
- A missing claim does not match, even when the user has the expected assignment
  in the provider's administrative UI.

Use the group name or ID exactly as it appears in the access token.
Temporal MCP does not query the identity provider for group membership. If the
group claim is absent, including when the provider emits an overage marker, no
authorization rule matches it.

### Token checks

Decode a test access token locally without sharing it or committing it to the
repository. Confirm:

- `iss` exactly equals an `auth.issuers[].issuer` value.
- `aud` contains the configured `audience`.
- The token is an access token intended for the MCP API, not an ID token for the
  client.
- `exp` and `nbf` make the token currently valid.
- The configured `claim_name` exists at the top level.
- Its value is a string or list containing the configured `claim_value` with
  exact case.

The server verifies the signature and registered claims itself. Decoding is a
diagnostic step, not a substitute for verification.

### Security

- Trust only issuers controlled for the intended user population.
- Give every issuer a specific audience for this API.
- Match centrally assigned claims rather than user-editable values.
- When several issuers are configured, put `issuer` on authorization rules that
  should apply to only one of them.
- Grant each profile only the tools and targets it needs.
- Do not place access tokens, API keys, or private keys in the YAML or
  documentation.
