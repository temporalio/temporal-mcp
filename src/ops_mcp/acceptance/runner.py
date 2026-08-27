"""Run live acceptance fixtures against an ephemeral Temporal dev server.

This module owns test process lifecycle only. It deliberately does not add any
fixture-specific field or filtering behavior to the MCP server.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

SessionNumber = Literal[1, 2, 3, 4]
StepAction = Literal["start", "run", "stop"]


@dataclass(frozen=True)
class FixtureStep:
    action: StepAction
    name: str
    module: str | None = None


SESSION_STEPS: dict[SessionNumber, tuple[FixtureStep, ...]] = {
    1: (
        FixtureStep("start", "session-1-worker", "ops_mcp.demo.worker"),
        FixtureStep("run", "session-1-seed", "ops_mcp.demo.seed"),
    ),
    2: (
        FixtureStep("start", "session-2-worker", "ops_mcp.demo_stuck.worker"),
        FixtureStep("run", "session-2-seed", "ops_mcp.demo_stuck.seed"),
    ),
    3: (
        FixtureStep("start", "session-3-v1-worker", "ops_mcp.demo_nondeterminism.worker_v1"),
        FixtureStep("run", "session-3-seed", "ops_mcp.demo_nondeterminism.seed"),
        FixtureStep("stop", "session-3-v1-worker"),
        FixtureStep("start", "session-3-v2-worker", "ops_mcp.demo_nondeterminism.worker_v2"),
        FixtureStep("run", "session-3-expose", "ops_mcp.demo_nondeterminism.expose"),
    ),
    4: (
        FixtureStep("start", "session-4-workflow-worker", "ops_mcp.demo_incident.workflow_worker"),
        FixtureStep("run", "session-4-seed", "ops_mcp.demo_incident.seed"),
        FixtureStep("start", "session-4-recovery-workers", "ops_mcp.demo_incident.activity_worker"),
    ),
}


class ManagedProcess(Protocol):
    pid: int

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...


PopenFactory = Callable[..., ManagedProcess]
RunFactory = Callable[..., subprocess.CompletedProcess[str]]


def fixture_steps(mode: str, focus_session: SessionNumber) -> tuple[FixtureStep, ...]:
    """Return fixture choreography without introducing a scenario data filter."""
    sessions: tuple[SessionNumber, ...]
    if mode == "isolated":
        sessions = (focus_session,)
    elif mode == "noisy":
        sessions = (1, 2, 3, 4)
    else:
        raise ValueError(f"Unsupported acceptance mode: {mode}")
    return tuple(step for session in sessions for step in SESSION_STEPS[session])


def reserve_local_port() -> int:
    """Ask the OS for a currently free loopback port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def build_server_command(temporal_binary: str, port: int) -> list[str]:
    """Build a non-persistent, headless dev-server command."""
    return [
        temporal_binary,
        "server",
        "start-dev",
        "--headless",
        "--ip",
        "127.0.0.1",
        "--port",
        str(port),
        "--search-attribute",
        "OrderId=Keyword",
    ]


def build_module_command(module: str) -> list[str]:
    return [sys.executable, "-c", f"from {module} import main; main()"]


def write_target_config(path: Path, port: int) -> None:
    """Point the normal target resolver at only the ephemeral acceptance server."""
    path.write_text(
        "\n".join(
            (
                "groups:",
                "  demo-support:",
                "    targets:",
                "      acceptance-local:",
                "        aliases:",
                "          - orders prod",
                "          - temporal dev",
                "          - acceptance local",
                "        account: local-acceptance",
                "        cluster: temporal-dev-child",
                "        namespace: default",
                f"        address: 127.0.0.1:{port}",
                "        tls: false",
                "",
            )
        ),
        encoding="utf-8",
    )


def child_environment(base: dict[str, str], port: int, target_config: Path) -> dict[str, str]:
    env = dict(base)
    env.update(
        {
            "TEMPORAL_ADDRESS": f"127.0.0.1:{port}",
            "TEMPORAL_NAMESPACE": "default",
            "OPS_MCP_TARGET_CONFIG": str(target_config),
            "OPS_MCP_DEMO_GROUP": "demo-support",
        }
    )
    for cloud_setting in ("TEMPORAL_API_KEY", "TEMPORAL_TLS"):
        env.pop(cloud_setting, None)
    return env


def wait_for_port(
    port: int,
    process: ManagedProcess,
    *,
    timeout_seconds: float = 30.0,
    connector: Callable[..., socket.socket] = socket.create_connection,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(f"Temporal dev server exited early with status {exit_code}")
        try:
            connection = connector(("127.0.0.1", port), timeout=0.2)
        except OSError:
            time.sleep(0.05)
        else:
            connection.close()
            return
    raise TimeoutError(f"Temporal dev server was not ready on 127.0.0.1:{port}")


def stop_process(process: ManagedProcess, *, timeout_seconds: float = 10.0) -> None:
    """Stop only the process group created by this harness."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=timeout_seconds)
    except ProcessLookupError:
        return


class FixtureOrchestrator:
    def __init__(
        self,
        *,
        environment: dict[str, str],
        log_directory: Path,
        popen: PopenFactory = subprocess.Popen,
        run: RunFactory = subprocess.run,
    ) -> None:
        self._environment = environment
        self._log_directory = log_directory
        self._popen = popen
        self._run = run
        self._processes: dict[str, ManagedProcess] = {}
        self._logs: list[object] = []

    def start(self, name: str, command: Sequence[str]) -> ManagedProcess:
        if name in self._processes:
            raise RuntimeError(f"Fixture process {name!r} is already running")
        log = (self._log_directory / f"{name}.log").open("w", encoding="utf-8")
        self._logs.append(log)
        process = self._popen(
            list(command),
            env=self._environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )
        self._processes[name] = process
        return process

    def run(self, name: str, command: Sequence[str]) -> None:
        result = self._run(
            list(command),
            env=self._environment,
            check=False,
            text=True,
        )
        if result.returncode:
            raise RuntimeError(f"Fixture step {name!r} failed with status {result.returncode}")

    def stop(self, name: str) -> None:
        process = self._processes.pop(name, None)
        if process is None:
            raise RuntimeError(f"Fixture process {name!r} was not started")
        stop_process(process)

    def apply(self, steps: Sequence[FixtureStep]) -> None:
        for step in steps:
            if step.action == "stop":
                self.stop(step.name)
                continue
            if step.module is None:
                raise RuntimeError(f"Fixture step {step.name!r} is missing a module")
            command = build_module_command(step.module)
            if step.action == "start":
                self.start(step.name, command)
                time.sleep(0.5)
                process = self._processes[step.name]
                if process.poll() is not None:
                    log_path = self._log_directory / f"{step.name}.log"
                    details = log_path.read_text(encoding="utf-8", errors="replace").strip()
                    tail = "\n".join(details.splitlines()[-10:])
                    raise RuntimeError(
                        f"Fixture process {step.name!r} exited during startup"
                        + (f":\n{tail}" if tail else "")
                    )
            else:
                self.run(step.name, command)

    def close(self) -> None:
        for process in reversed(tuple(self._processes.values())):
            stop_process(process)
        self._processes.clear()
        for log in self._logs:
            close = getattr(log, "close", None)
            if close is not None:
                close()
        self._logs.clear()


def run_acceptance(
    *,
    mode: str,
    focus_session: SessionNumber,
    command: Sequence[str],
    temporal_binary: str,
    port: int | None = None,
) -> int:
    if not command:
        raise ValueError("An agent command is required after '--'")
    selected_port = port if port is not None else reserve_local_port()
    with tempfile.TemporaryDirectory(prefix="ops-mcp-acceptance-") as temporary:
        temporary_path = Path(temporary)
        target_config = temporary_path / "targets.yaml"
        write_target_config(target_config, selected_port)
        environment = child_environment(os.environ, selected_port, target_config)
        orchestrator = FixtureOrchestrator(
            environment=environment,
            log_directory=temporary_path,
        )
        try:
            server = orchestrator.start(
                "temporal-server",
                build_server_command(temporal_binary, selected_port),
            )
            wait_for_port(selected_port, server)
            orchestrator.apply(fixture_steps(mode, focus_session))
            print(
                f"Acceptance fixture ready: mode={mode}, focus_session={focus_session}, "
                f"Temporal=127.0.0.1:{selected_port}",
                flush=True,
            )
            result = subprocess.run(list(command), env=environment, check=False)
            return result.returncode
        finally:
            orchestrator.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run an agent acceptance command against fresh Temporal state, or against a "
            "fresh namespace containing all fixtures as genuine investigation noise."
        )
    )
    parser.add_argument("--mode", choices=("isolated", "noisy"), default="isolated")
    parser.add_argument("--session", type=int, choices=(1, 2, 3, 4), required=True)
    parser.add_argument("--port", type=int, help="Explicit child-server port (default: free port)")
    parser.add_argument(
        "--temporal-binary",
        default=shutil.which("temporal") or "temporal",
        help="Temporal CLI executable",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main() -> None:
    args = _parser().parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        exit_code = run_acceptance(
            mode=args.mode,
            focus_session=args.session,
            command=command,
            temporal_binary=args.temporal_binary,
            port=args.port,
        )
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        print(f"acceptance infrastructure error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
