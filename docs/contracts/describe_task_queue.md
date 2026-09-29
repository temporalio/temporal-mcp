# `describe_task_queue`

- **Operator question:** Are workers polling this task queue, and what is its
  approximate dispatch backlog?
- **Inputs:** `target_ref`, `task_queue`, `task_type: workflow | activity | both`.
- **Output:** poller count/identities and last-access times when available;
  approximate backlog count/age and server-provided rate/stat fields.
- **Facts:** current `DescribeTaskQueue` response. **Interpretation:** a narrowly
  named `currently_has_pollers`; never historical health or root cause.
- **Gateway calls:** `DescribeTaskQueue` with stats and pollers for the requested
  task type. `both` makes two bounded calls and preserves an independent result or structured
  error for each kind.
- **Completeness:** `complete` for the current snapshot; approximate fields remain
  labeled approximate.
- **Limits/refusals:** require task type; redact/limit worker identity if configured;
  unsupported enhanced stats return capability warnings.
- **Errors:** invalid argument, unsupported capability, shared target/connection.
- **Scenario assertions:** distinguish workflow and activity pollers; an empty
  poller list says only that no pollers were visible at observation time. Backlog
  is approximate task count, not an exact count of unique workflows.
- **Open decisions:** identity redaction default for the POC.
