# Reset event semantics: pinned POC decision

Status: accepted for the pinned compatibility baseline only.

## Baseline and evidence

Validated against:

- Temporal CLI `1.7.0`
- CLI-bundled self-hosted dev server `1.31.0`
- Temporal Python SDK `1.31.0`

The CLI help defines `--event-id` as the event ID to reset to, occurring after a
`WorkflowTaskStarted`, and names `WorkflowTaskCompleted` and
`WorkflowTaskFailed` as examples. The gated integration test
`tests/integration/test_reset_event_semantics.py` proves the behavior end to end
by issuing real CLI resets against disposable runs.

## Decision

For this compatibility baseline, the reset analyzer may enumerate exactly these
history event classes as reset points:

- `WorkflowTaskCompleted`
- `WorkflowTaskFailed`
- `WorkflowTaskTimedOut`

The value passed to CLI `--event-id` is the **exact event ID of that workflow-task
finish event**. It is not the preceding `WorkflowTaskStarted` ID, the next event
ID, or an activity/closing event ID.

The integration test establishes:

1. A `WorkflowTaskCompleted` event ID succeeds.
2. A reset-generated `WorkflowTaskFailed` event ID succeeds.
3. A naturally timed-out workflow task's `WorkflowTaskTimedOut` event ID succeeds.
4. A `WorkflowExecutionCompleted` event ID is rejected as not being at a reset
   boundary.
5. Inspecting the accepted reset run shows the selected finish event replaced by
   a `WorkflowTaskFailed` event with cause `RESET_WORKFLOW`; history before that
   boundary is retained and a new workflow task is scheduled.

## Analyzer constraint

The analyzer should use an explicit allowlist of the three event classes above,
not a name-prefix match and not “every event after WorkflowTaskStarted.” It should
emit the selected finish event's ID unchanged in `command_preview`.

This resolves event-boundary calculation only. It does **not** establish that a
point is operationally safe. Recommendation still requires complete history and
the separate replay, versioning, side-effect, signal/update, child-workflow,
external-workflow, parallel-command, and continue-as-new safety checks in the
tool contract.

## Revalidation

Run:

```bash
temporal server start-dev --headless
OPS_MCP_INTEGRATION=1 uv run pytest \
  tests/integration/test_reset_event_semantics.py -q
```

The test intentionally creates reset runs and therefore belongs only against a
disposable dev namespace. Revalidate before adding another server/CLI version to
the compatibility matrix.
