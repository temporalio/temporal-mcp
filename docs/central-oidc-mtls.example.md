```yaml
# The centrally hosted MCP endpoint. Restrict network access according to your
# internal security model.
listen: 0.0.0.0:8443

# Serve the MCP endpoint over HTTPS.
listen_tls:
  cert_file: /etc/temporal-mcp/server/tls.crt # Example certificate path.
  key_file: /etc/temporal-mcp/server/tls.key # Example key path.

# User-to-MCP authentication. The issuer and audience must match the access
# token exactly. The audience identifies this API, not the MCP client app.
auth:
  resource: https://temporal-mcp.example.com/mcp # Example MCP URL.
  issuers:
    - issuer: https://id.example.com/ # Example identity provider.
      audience: api://temporal-mcp # Example API audience.
  scopes_supported:
    - temporal-mcp.access # Example scope configured in the identity provider.

# User-to-tool-and-target authorization. A token claim assigns a profile; this
# file defines what that profile can use inside this MCP server.
authorization:
  profiles:
    mcp-user: # Example profile name.
      tools:
        - list_available_targets
        - check_connection
        - get_workflows
        - get_workflow_history
        - get_task_queues
        - search_workflows
        - count_workflows
        - list_search_attributes
      targets:
        - payments-production # Example target name.

  rules:
    - claim_name: groups # Example group claim in the access token.
      claim_value: engineering # Example existing group name or ID.
      profile: mcp-user # Refers to the profile above.

# MCP-to-Temporal authentication. Each target uses a certificate available only
# on the server host. End users never receive these files.
targets:
  - name: payments-production # Example target name.
    description: Production namespace of the payments service.
    address: payments-production.a1b2c.tmprl.cloud:7233 # Example endpoint.
    namespace: payments-production.a1b2c # Example namespace.
    tls:
      cert_file: /etc/temporal-mcp/targets/payments-production.crt # Example certificate path.
      key_file: /etc/temporal-mcp/targets/payments-production.key # Example key path.
    payload_handling: passthrough

# Keep audit records separate from diagnostic logs and collect both according
# to your organization's retention policy.
audit_log_file: /var/log/temporal-mcp/audit.jsonl
error_log_file: /var/log/temporal-mcp/server.log
```
