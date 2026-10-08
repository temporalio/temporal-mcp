from __future__ import annotations

import json
import signal
import subprocess
from pathlib import Path

import pytest

from ops_mcp.acceptance.runner import (
    FixtureOrchestrator,
    build_server_command,
    child_environment,
    fixture_steps,
    render_scenario,
    scenario_command,
    stop_process,
    write_target_config,
)


class FakeProcess:
    def __init__(self, pid: int = 321, exit_code: int | None = None) -> None:
        self.pid = pid
        self.exit_code = exit_code
        self.wait_timeouts: list[float | None] = []

    def poll(self) -> int | None:
        return self.exit_code

    def wait(self, timeout: float | None = None) -> int:
        self.wait_timeouts.append(timeout)
        self.exit_code = 0
        return 0


def test_isolated_mode_only_prepares_requested_session() -> None:
    steps = fixture_steps("isolated", 3)
    assert [step.name for step in steps] == [
        "session-3-v1-worker",
        "session-3-seed",
        "session-3-v1-worker",
        "session-3-v2-worker",
        "session-3-expose",
    ]


def test_noisy_mode_seeds_all_real_fixtures_without_a_filter_field() -> None:
    steps = fixture_steps("noisy", 4)
    assert {step.name.split("-")[1] for step in steps} == {"1", "2", "3", "4"}
    serialized = repr(steps).casefold()
    assert "scenarioid" not in serialized
    assert "scenario_id" not in serialized


def test_server_is_in_memory_headless_and_registers_search_attributes() -> None:
    command = build_server_command("/usr/local/bin/temporal", 17433)
    assert command[:3] == ["/usr/local/bin/temporal", "server", "start-dev"]
    assert "--headless" in command
    assert command[command.index("--port") + 1] == "17433"
    assert "--db-filename" not in command
    assert {"OrderId=Keyword", "Team=Keyword", "Domain=Keyword"}.issubset(command)


def test_generated_config_and_environment_point_only_at_child_server(tmp_path: Path) -> None:
    config = tmp_path / "operator-mcp.yaml"
    metadata = tmp_path / "metadata.json"
    write_target_config(config, 17433)
    env = child_environment(
        {"TEMPORAL_API_KEY": "must-not-leak", "TEMPORAL_TLS": "true", "UNRELATED": "kept"},
        17433,
        config,
        metadata,
    )
    config_text = config.read_text(encoding="utf-8")
    assert "name: orders-prod" in config_text
    assert "127.0.0.1:17433" in config_text
    assert "listen: 127.0.0.1:0" in config_text
    assert env["TEMPORAL_ADDRESS"] == "127.0.0.1:17433"
    assert env["OPS_MCP_ACCEPTANCE_CONFIG"] == str(config)
    assert env["OPS_MCP_ACCEPTANCE_METADATA"] == str(metadata)
    assert env["UNRELATED"] == "kept"
    assert "TEMPORAL_API_KEY" not in env
    assert "TEMPORAL_TLS" not in env


def test_rendered_scenario_launches_go_binary_and_uses_dynamic_prompt(tmp_path: Path) -> None:
    template = tmp_path / "template.yaml"
    template.write_text(
        "command: __OPERATOR_MCP_JSON__\n"
        "cwd: __REPO_ROOT_JSON__\n"
        "config: __TARGET_CONFIG_JSON__\n"
        "prompt: __DYNAMIC_PROMPT_JSON__\n",
        encoding="utf-8",
    )
    metadata = tmp_path / "metadata.json"
    metadata.write_text(
        json.dumps({"session_3": {"prompt": "A deploy prompt at 2026-01-01T00:00:00Z"}}),
        encoding="utf-8",
    )
    destination = tmp_path / "scenario.yaml"
    render_scenario(
        template,
        destination,
        focus_session=3,
        operator_mcp=Path("/repo/bin/operator-mcp"),
        repo_root=Path("/repo"),
        target_config=Path("/tmp/operator-mcp.yaml"),
        metadata_path=metadata,
    )
    rendered = destination.read_text(encoding="utf-8")
    assert 'command: "/repo/bin/operator-mcp"' in rendered
    assert 'prompt: "A deploy prompt at 2026-01-01T00:00:00Z"' in rendered
    assert "__" not in rendered


def test_scenario_command_requires_and_replaces_token(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario.yaml"
    assert scenario_command(["npm", "run", "scenario", "--", "{scenario}"], scenario)[-1] == str(
        scenario
    )
    with pytest.raises(ValueError, match="must contain"):
        scenario_command(["npm", "run", "scenario"], scenario)


def test_stop_process_targets_only_owned_process_group(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess(pid=4321)
    calls: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(
        "ops_mcp.acceptance.runner.os.killpg", lambda pid, sig: calls.append((pid, sig))
    )
    stop_process(process)
    assert calls == [(4321, signal.SIGTERM)]
    assert process.wait_timeouts == [10.0]


def test_stop_process_escalates_only_after_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess(pid=4321)
    wait_calls = 0

    def wait(timeout: float | None = None) -> int:
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            raise subprocess.TimeoutExpired("worker", timeout)
        process.exit_code = 0
        return 0

    process.wait = wait  # type: ignore[method-assign]
    calls: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(
        "ops_mcp.acceptance.runner.os.killpg", lambda pid, sig: calls.append((pid, sig))
    )
    stop_process(process)
    assert calls == [(4321, signal.SIGTERM), (4321, signal.SIGKILL)]


def test_orchestrator_runs_choreography_and_reaps_started_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[list[str]] = []
    run_commands: list[list[str]] = []
    processes: list[FakeProcess] = []

    def popen(command: list[str], **_: object) -> FakeProcess:
        started.append(command)
        process = FakeProcess(pid=5000 + len(processes))
        processes.append(process)
        return process

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        run_commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    orchestrator = FixtureOrchestrator(environment={}, log_directory=tmp_path, popen=popen, run=run)
    monkeypatch.setattr("ops_mcp.acceptance.runner.os.killpg", lambda _pid, _sig: None)
    orchestrator.apply(fixture_steps("isolated", 3))
    orchestrator.close()
    assert len(started) == 2
    assert len(run_commands) == 2
    assert all(process.exit_code == 0 for process in processes)
