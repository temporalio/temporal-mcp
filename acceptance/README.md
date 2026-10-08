# Interactive agent acceptance

This directory contains the four historical user-test sessions from the
prototype. The Temporal fixtures stay in Python because they are test data, not
part of the Go MCP runtime. Every run tests the current Go `operator-mcp`
binary.

## Contents

```text
acceptance/
├── scenarios/                 # Tracked IAI scenario templates
├── src/ops_mcp/acceptance/    # Temporal/process orchestration
├── src/ops_mcp/demo/          # Session 1: failed order and recovery cohort
├── src/ops_mcp/demo_stuck/    # Session 2: activity backlog
├── src/ops_mcp/demo_nondeterminism/ # Session 3: v1/v2 replay divergence
├── src/ops_mcp/demo_incident/ # Session 4: incident-window population
└── tests/                     # Harness and fixture-contract tests
```

The tracked scenario files are templates. The runner replaces the Go binary,
temporary config, repository path, and the dynamic Session 3/4 timestamps
before invoking IAI. IAI copies that rendered input into its run artifacts.

## Prerequisites

- Go and the dependencies required by the root module.
- Python 3.11 or later and `uv`.
- Temporal CLI on `PATH`.
- A local checkout of Interactive Agent Interface.

IAI currently accepts stdio MCP targets. The Go binary therefore supports
`--stdio` for these tests; its default transport remains streamable HTTP.

## Build and test

From the repository root:

```sh
go build -o bin/operator-mcp .
uv sync --project acceptance --group dev
uv run --project acceptance pytest
```

## Run a session with IAI

Substitute the absolute path to the directory containing IAI's `package.json`:

```sh
uv run --project acceptance operator-mcp-acceptance \
  --session 1 \
  -- \
  npm --prefix /absolute/path/to/interactive-agent-interface \
  run scenario -- {scenario} \
  --out /absolute/path/to/operator-mcp/acceptance/runs
```

`{scenario}` is a literal token. The runner replaces it with the rendered,
temporary scenario path.

Use `--session 2`, `3`, or `4` for another conversation. Add `--mode noisy` to
seed all four fixture populations while keeping the selected session's prompt:

```sh
uv run --project acceptance operator-mcp-acceptance \
  --mode noisy --session 4 \
  -- \
  npm --prefix /absolute/path/to/interactive-agent-interface \
  run scenario -- {scenario} \
  --out /absolute/path/to/operator-mcp/acceptance/runs
```

The runner owns and stops the ephemeral Temporal server and every fixture
worker, including on failure. Session 4 includes an explicit readiness barrier:
IAI does not start until all 160 runs have recorded their recovery outcome, so
the consumer test cannot race the activity workers.

### Use an interactive driver

The tracked scenarios are scripted for reproducible regression testing. To let
IAI generate natural follow-ups, copy a scenario and replace `prompts` with:

```yaml
driver:
  type: agent
  goal: Follow up until the order failure and safe next step are understood.
  initialPrompt: In orders prod, why did order 88231 fail?
  maxTurns: 4
```

Run the copy with `--scenario /path/to/session-driver.yaml`; keep the rest of
the command unchanged. For Sessions 3 and 4, use
`initialPrompt: __DYNAMIC_PROMPT_JSON__` so the runner inserts the seeded
timestamp. The driver consumes a second IAI/Codex session and writes the same
audit artifacts under `acceptance/runs`.

## Session contracts

| Session | Seeded state | Historical prompt focus |
|---|---|---|
| 1 | Failed order `88231` and 12 completed comparison orders | Failure diagnosis, recovery evidence, read-only reset planning |
| 2 | 1,830 running workflows and 4,200 queued activities | Exact-run stuckness and current dispatch evidence |
| 3 | 230 candidates, 190 exposed to v2, 43 nondeterministic | Population discovery and history-confirmed nondeterminism |
| 4 | 160 runs, six teams, three domains, 90 payment runs | Incident impact, ownership, and money-touching classification |

The prompts intentionally retain requests that the current Go tool surface may
not fully satisfy. An agent should state those limitations rather than invent a
capability or imply that it performed a write.
