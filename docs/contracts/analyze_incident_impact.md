# `analyze_incident_impact`

`analyze_incident_impact` is a read-only historical correlation intent tool. It reuses the same
durable activity-dependency evidence collector as `inspect_activity_queue_dependencies`, then adds
incident-window classification. It does not use scenario-specific fields or current queue state.

The caller supplies `candidate_query` and an absolute incident window, and may supply independently known affected
activity task queues and an expected-duration policy. When queues are omitted, the bounded
dependency collector discovers queues from exact pending-activity and scheduling history across
the complete continuation chain. It does not use `business_grouping` fields to discover the
candidate boundary. A run is
**confirmed historical impact** only
when its history shows one of these queue-linked facts:

- an activity on an affected queue was scheduled before the incident ended and started only after
  the incident ended; or
- an activity on an affected queue failed or timed out during the incident window.

Workflow failures during the window and runs exceeding the supplied duration are returned as
supporting findings, but do not promote a run to confirmed. Temporal correlation is evidence of
impact, not proof that the incident caused it.

The response keeps these quantities separate:

- visibility candidate frame;
- histories inspected;
- history-confirmed impact;
- inspected but unconfirmed;
- unknown/truncated or failed inspections; and
- uninspected candidates.

Confirmed group breakdowns (workflow type plus requested custom Search Attributes such as `Team`
or `Domain`) are calculated only from confirmed runs. Values are returned as raw registered
Search Attribute values; the server does not infer team ownership or monetary meaning. Candidate
rows never enter those totals.

`exact_confirmed_count` is true only after a complete candidate census with complete histories and
no inspection failures. Otherwise `confirmed_affected_lower_bound` is the only safe population
claim. A sample must not be presented as an exact count unless an external statistical method and
its assumptions are explicitly supplied outside this tool.

Current `describe_task_queue` data is deliberately excluded. Current pollers and backlog cannot
establish which workers were polling or whether dispatch stopped during a past incident; that
requires historical metrics or logs through a separate observability integration.

The server adds an overlap predicate to the supplied Visibility scope: candidates must have
started before the incident ended and either closed after it began or still be running. The caller
does not need to reproduce this time predicate in `candidate_query`.

The response separates `candidate_population`, `impact_criteria`, and `business_grouping`. Its
`scope` names the candidate query, affected queues, whether queues were explicit or discovered,
any discovered queues excluded by an explicit selection, and two independent completeness flags.
`candidate_population_complete=true` never implies `incident_impact_complete=true` for an
explicit queue subset. `page` reports the current call; `cumulative` reports the uninterrupted
continuation chain; the top-level `continuation_token` is the only supported resume instruction.
