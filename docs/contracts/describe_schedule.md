# `describe_schedule`

- **Operator question:** What will this schedule do, is it paused, and when did/will
  it run?
- **Inputs:** `target_ref`, `schedule_id`, optional `upcoming_count: 0..50` and
  bounded time range.
- **Output:** action/spec/policies/state, paused note, recent actions, and computed
  upcoming matching times.
- **Facts:** schedule description and matching-time response. **Interpretation:**
  normalized schedule-shape labels only.
- **Gateway calls:** `DescribeSchedule`; `ListScheduleMatchingTimes` only when
  upcoming times are requested.
- **Completeness:** `complete` for description; upcoming list complete only for the
  requested bounded range/count.
- **Limits/refusals:** cap times/range/output; decoded action inputs follow payload
  redaction and size rules.
- **Errors:** schedule not found, unsupported capability, codec/decode, shared.
- **Scenario assertions:** paused state, workflow action, recent run, and next
  bounded times match the server.
- **Open decisions:** whether action inputs belong in v1 or are metadata-only.
