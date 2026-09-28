from __future__ import annotations

import signal
import subprocess
from pathlib import Path

import pytest

from ops_mcp.acceptance.runner import (
    FixtureOrchestrator,
    build_server_command,
    child_environment,
    fixture_steps,
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


def test_server_is_in_memory_headless_and_uses_an_explicit_port() -> None:
    command = build_server_command("/usr/local/bin/temporal", 17433)

    assert command[:3] == ["/usr/local/bin/temporal", "server", "start-dev"]
    assert "--headless" in command
    assert command[command.index("--port") + 1] == "17433"
    assert "--db-filename" not in command


def test_generated_target_and_environment_point_only_at_child_server(tmp_path: Path) -> None:
    config = tmp_path / "targets.yaml"
    write_target_config(config, 17433)
    env = child_environment(
        {
            "TEMPORAL_API_KEY": "must-not-leak",
            "TEMPORAL_TLS": "true",
            "UNRELATED": "kept",
        },
        17433,
        config,
    )

    config_text = config.read_text(encoding="utf-8")
    assert "127.0.0.1:17433" in config_text
    assert "Scenario" not in config_text
    assert env["TEMPORAL_ADDRESS"] == "127.0.0.1:17433"
    assert env["OPS_MCP_TARGET_CONFIG"] == str(config)
    assert env["OPS_MCP_GROUP"] == "demo-support"
    assert env["UNRELATED"] == "kept"
    assert "TEMPORAL_API_KEY" not in env
    assert "TEMPORAL_TLS" not in env


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
