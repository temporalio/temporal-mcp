# `check_connection`

- **Operator question:** Can this configured target reach its Temporal namespace
  and configured payload codec, and where does setup fail?
- **Inputs:** `target_ref: str`; `probe_codec: bool = true`.
- **Output:** stage results for `server`, `namespace`, and optionally `codec`, each
  with `status: reachable | failed | not_configured | not_checked`, latency, and a
  typed failure code; non-secret server capability facts.
- **Facts:** RPC/HTTP outcomes and namespace identity. **Interpretation:** one
  `overall_status`; no claims about application workers or workflow health.
- **Gateway calls:** `GetSystemInfo`, `DescribeNamespace`; bounded HTTP codec
  health/probe call only to the trusted configured endpoint.
- **Completeness:** `complete` only when every configured stage is attempted;
  otherwise `budget_exhausted` or `unknown`, with per-stage status.
- **Limits/refusals:** fixed deadlines and response-size ceiling; never return
  addresses, credentials, TLS material, or codec response bodies; refuse an
  untrusted codec URL.
- **Errors:** shared connection, namespace, codec, deadline, and capability
  errors. Expected stage failures belong in the structured answer, not a vague
  top-level exception.
- **Scenario assertions:** stopped server identifies `server`; missing namespace
  identifies `namespace`; bad codec identifies `codec`; no response claims API
  key support is disabled without direct evidence.
- **Open decisions:** define the codec probe protocol before implementation.
