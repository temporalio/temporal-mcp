# `resolve_target`

- **Operator question:** Which configured Temporal target does this human
  environment name mean for the current caller, or which targets may I inspect?
- **Inputs:** optional `human_name: str`; identity comes from request context, not arguments.
- **Output:** a discriminated `answer`. An exact unique match has `kind: resolved`,
  `target_ref`, display name, optional account and cluster labels, namespace, and
  `resolved_from`. Discovery or an exact ambiguous match has `kind: candidates`,
  a reason, and deterministically ordered public candidate summaries. Connection
  addresses, TLS configuration, credential configuration, and secrets never cross
  this boundary. If exactly one target is authorized and no name is supplied, the response may
  resolve it directly with `resolution: sole_authorized_target`.
- **Facts:** selected configured mapping. **Interpretation:** normalized alias
  matching only; no environment inference beyond configured aliases.
- **Gateway calls:** none in the POC. Target existence probing belongs to
  `check_connection`.
- **Completeness:** `complete` for the caller-visible catalog snapshot.
- **Limits/refusals:** exact normalized target-reference/alias matching only; no
  fuzzy or partial matching. Omitted names list unique authorized targets (or resolve the sole
  authorized target),
  deduplicated across grants. Exact ambiguous matches return authorized candidates
  rather than an error. Matching never scans unauthorized groups.
- **Errors:** `invalid_argument`, `target_not_found`. Unknown and unauthorized names
  are deliberately indistinguishable. `target_not_authorized` remains applicable
  when another tool consumes a `target_ref`, not to name discovery.
- **Scenario assertions:** `orders prod` resolves deterministically to one opaque
  target; no name lists only authorized targets; duplicate grants produce one
  candidate; an unauthorized alias does not reveal whether the target exists.
- **Schema-v1 compatibility:** exact resolution retains the existing fields inside
  `answer` and an in-process `response.target_ref` property. The duplicated top-level
  serialized `target_ref` was removed so discovery is a clean discriminated result.
  Consumers reading that duplicated wire field must move to `answer.target_ref`.
- **Open decisions:** none blocking; authentication will replace the demo identity
  source later.
