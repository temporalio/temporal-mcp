# `list_schedules`

- **Operator question:** Which schedules match this name, schedule-shape, or paused
  filter?
- **Inputs:** `target_ref`; optional `name_contains`, `shape: cron | interval |
  calendar`, `paused`; `page_size`, `continuation_token`.
- **Output:** bounded schedule summaries with ID, paused state, shape summary,
  recent/next actions if supplied by list response, and continuation token.
- **Facts:** list response. **Interpretation:** local shape/paused filtering.
- **Gateway calls:** paged `ListSchedules`.
- **Completeness:** page completeness plus explicit server pages scanned; local
  filtering that stops at a scan ceiling is `truncated`, not complete.
- **Limits/refusals:** cap server pages examined for local filters and output size;
  do not fetch every schedule detail implicitly.
- **Errors:** invalid token/filter, unsupported capability, shared errors.
- **Scenario assertions:** paused cron schedule matches all three filters; scan
  ceiling visibly marks partial results.
- **Open decisions:** whether `page_size` means matches returned or server rows scanned.
