"""Live mTLS coverage against a self-hosted Temporal frontend."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import ssl
import subprocess
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from google.protobuf.duration_pb2 import Duration
from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest

from ops_mcp.acceptance.runner import reserve_local_port
from ops_mcp.clients import CachingTemporalClientProvider
from ops_mcp.facts.connection import ConnectionStageStatus
from ops_mcp.gateways.connection import SdkConnectionGateway
from ops_mcp.runtime.budget import BasicExecutionBudget
from ops_mcp.runtime.context import Principal, RequestContext
from ops_mcp.targets import YamlTargetCatalog

pytestmark = pytest.mark.skipif(
    os.getenv("OPS_MCP_MTLS_INTEGRATION") != "1",
    reason="set OPS_MCP_MTLS_INTEGRATION=1 to run the self-hosted Temporal mTLS test",
)


async def test_live_temporal_frontend_requires_and_uses_mtls(tmp_path: Path) -> None:
    server_binary_value = os.getenv("OPS_MCP_TEMPORAL_SERVER_BINARY")
    server_binary = server_binary_value or shutil.which("temporal-server")
    openssl_binary = shutil.which("openssl")
    if server_binary is None or openssl_binary is None:
        pytest.fail(
            "temporal-server and openssl are required; set "
            "OPS_MCP_TEMPORAL_SERVER_BINARY when temporal-server is not on PATH"
        )

    ca_cert, server_cert, server_key, client_cert, client_key = _generate_certificates(
        tmp_path, openssl_binary
    )
    frontend_port = reserve_local_port()
    server_config = tmp_path / "temporal-server.yaml"
    _write_server_config(
        server_config,
        frontend_port=frontend_port,
        ca_cert=ca_cert,
        server_cert=server_cert,
        server_key=server_key,
        system_worker_cert=client_cert,
        system_worker_key=client_key,
    )
    temporal_log = (tmp_path / "temporal.log").open("w+", encoding="utf-8")
    temporal = await asyncio.create_subprocess_exec(
        server_binary,
        "--config-file",
        str(server_config),
        "--allow-no-auth",
        "start",
        stdout=temporal_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    provider = CachingTemporalClientProvider(connection_attempt_timeout=15)

    try:
        await _wait_for_port(frontend_port, temporal)
        await _assert_client_certificate_is_required(frontend_port, ca_cert)

        target_config = tmp_path / "targets.yaml"
        target_config.write_text(
            "\n".join(
                (
                    "groups:",
                    "  integration:",
                    "    targets:",
                    "      mtls-local:",
                    "        account: live-test",
                    "        cluster: temporal-self-hosted",
                    "        namespace: default",
                    f"        address: 127.0.0.1:{frontend_port}",
                    "        tls: true",
                    "        server_name: localhost",
                    f"        client_cert_path: {client_cert}",
                    f"        client_key_path: {client_key}",
                    f"        server_ca_cert_path: {ca_cert}",
                    "",
                )
            ),
            encoding="utf-8",
        )
        target = YamlTargetCatalog.load(target_config).resolve(
            Principal("live-test", ("integration",)), "mtls-local"
        )
        client = await provider.get_client(target.target_ref, target.connection)
        await client.workflow_service.register_namespace(
            RegisterNamespaceRequest(
                namespace="default",
                workflow_execution_retention_period=Duration(seconds=86_400),
            )
        )
        context = RequestContext(
            "live-mtls",
            Principal("live-test", ("integration",)),
            BasicExecutionBudget(deadline=datetime.now(UTC) + timedelta(seconds=30)),
        )
        gateway = SdkConnectionGateway(provider)

        server = await gateway.probe_server(context, target)
        namespace = await gateway.probe_namespace(context, target)

        assert server.outcome.status is ConnectionStageStatus.REACHABLE
        assert server.server_version
        assert namespace.outcome.status is ConnectionStageStatus.REACHABLE
        assert namespace.namespace == "default"
    finally:
        await provider.shutdown()
        await _stop_process(temporal)
        temporal_log.close()


async def _assert_client_certificate_is_required(port: int, ca_cert: Path) -> None:
    context = ssl.create_default_context(cafile=ca_cert)
    context.set_alpn_protocols(["h2"])
    writer: asyncio.StreamWriter | None = None
    rejected = False
    try:
        async with asyncio.timeout(5):
            reader, writer = await asyncio.open_connection(
                "127.0.0.1",
                port,
                ssl=context,
                server_hostname="localhost",
            )
            writer.write(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            await writer.drain()
            rejected = await reader.read(1) == b""
    except (ConnectionError, ssl.SSLError):
        rejected = True
    finally:
        if writer is not None:
            writer.close()
            with suppress(ConnectionError, ssl.SSLError, TimeoutError):
                async with asyncio.timeout(1):
                    await writer.wait_closed()
    assert rejected


async def _wait_for_port(
    port: int, process: asyncio.subprocess.Process, timeout_seconds: float = 60
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.returncode is not None:
            raise RuntimeError(f"Temporal server exited with status {process.returncode}")
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            await asyncio.sleep(0.1)
            continue
        writer.close()
        await writer.wait_closed()
        return
    raise TimeoutError("Temporal frontend did not become ready")


def _write_server_config(
    path: Path,
    *,
    frontend_port: int,
    ca_cert: Path,
    server_cert: Path,
    server_key: Path,
    system_worker_cert: Path,
    system_worker_key: Path,
) -> None:
    dynamic_config = path.with_name("dynamic-config.yaml")
    dynamic_config.write_text("{}\n", encoding="utf-8")
    ports = iter(reserve_local_port() for _ in range(10))
    frontend_membership_port = next(ports)
    frontend_http_port = next(ports)
    matching_grpc_port = next(ports)
    matching_membership_port = next(ports)
    history_grpc_port = next(ports)
    history_membership_port = next(ports)
    worker_grpc_port = next(ports)
    worker_membership_port = next(ports)
    pprof_port = next(ports)
    metrics_port = next(ports)
    config = {
        "log": {"stdout": True, "level": "warn"},
        "persistence": {
            "defaultStore": "sqlite-default",
            "visibilityStore": "sqlite-visibility",
            "numHistoryShards": 1,
            "datastores": {
                "sqlite-default": {"sql": _sqlite_config("default")},
                "sqlite-visibility": {"sql": _sqlite_config("visibility")},
            },
        },
        "global": {
            "membership": {
                "maxJoinDuration": "30s",
                "broadcastAddress": "127.0.0.1",
            },
            "pprof": {"port": pprof_port},
            "metrics": {
                "prometheus": {
                    "framework": "tally",
                    "timerType": "histogram",
                    "listenAddress": f"127.0.0.1:{metrics_port}",
                }
            },
            "tls": {
                "frontend": {
                    "server": {
                        "requireClientAuth": True,
                        "certFile": str(server_cert),
                        "keyFile": str(server_key),
                        "clientCaFiles": [str(ca_cert)],
                    }
                },
                "systemWorker": {
                    "certFile": str(system_worker_cert),
                    "keyFile": str(system_worker_key),
                    "client": {
                        "serverName": "localhost",
                        "rootCaFiles": [str(ca_cert)],
                    },
                },
            },
        },
        "services": {
            "frontend": {
                "rpc": {
                    "grpcPort": frontend_port,
                    "membershipPort": frontend_membership_port,
                    "bindOnLocalHost": True,
                    "httpPort": frontend_http_port,
                }
            },
            "matching": {
                "rpc": {
                    "grpcPort": matching_grpc_port,
                    "membershipPort": matching_membership_port,
                    "bindOnLocalHost": True,
                }
            },
            "history": {
                "rpc": {
                    "grpcPort": history_grpc_port,
                    "membershipPort": history_membership_port,
                    "bindOnLocalHost": True,
                }
            },
            "worker": {
                "rpc": {
                    "grpcPort": worker_grpc_port,
                    "membershipPort": worker_membership_port,
                    "bindOnLocalHost": True,
                }
            },
        },
        "clusterMetadata": {
            "enableGlobalNamespace": False,
            "failoverVersionIncrement": 10,
            "masterClusterName": "active",
            "currentClusterName": "active",
            "clusterInformation": {
                "active": {
                    "enabled": True,
                    "initialFailoverVersion": 1,
                    "rpcName": "frontend",
                    "rpcAddress": f"localhost:{frontend_port}",
                    "httpAddress": f"localhost:{frontend_http_port}",
                }
            },
        },
        "dcRedirectionPolicy": {"policy": "noop"},
        "archival": {
            "history": {"state": "disabled", "enableRead": False},
            "visibility": {"state": "disabled", "enableRead": False},
        },
        "namespaceDefaults": {
            "archival": {
                "history": {"state": "disabled"},
                "visibility": {"state": "disabled"},
            }
        },
        "dynamicConfigClient": {
            "filepath": str(dynamic_config),
            "pollInterval": "10s",
        },
    }
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _sqlite_config(database_name: str) -> dict[str, object]:
    return {
        "user": "",
        "password": "",
        "pluginName": "sqlite",
        "databaseName": database_name,
        "connectAddr": "localhost",
        "connectProtocol": "tcp",
        "connectAttributes": {"mode": "memory", "cache": "private"},
        "maxConns": 1,
        "maxIdleConns": 1,
        "maxConnLifetime": "1h",
        "tls": {
            "enabled": False,
            "caFile": "",
            "certFile": "",
            "keyFile": "",
            "enableHostVerification": False,
            "serverName": "",
        },
    }


def _generate_certificates(
    directory: Path, openssl_binary: str
) -> tuple[Path, Path, Path, Path, Path]:
    ca_cert = directory / "ca.pem"
    ca_key = directory / "ca.key"
    server_cert = directory / "server.pem"
    server_key = directory / "server.key"
    server_request = directory / "server.csr"
    client_cert = directory / "client.pem"
    client_key = directory / "client.key"
    client_request = directory / "client.csr"
    commands = (
        (
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-days",
            "1",
            "-subj",
            "/CN=Ops MCP live test CA",
        ),
        (
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            str(server_key),
            "-out",
            str(server_request),
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext",
            "extendedKeyUsage=serverAuth",
        ),
        (
            "x509",
            "-req",
            "-in",
            str(server_request),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_cert),
            "-days",
            "1",
            "-sha256",
            "-copy_extensions",
            "copy",
        ),
        (
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-keyout",
            str(client_key),
            "-out",
            str(client_request),
            "-subj",
            "/CN=ops-mcp-live-test",
            "-addext",
            "extendedKeyUsage=clientAuth",
        ),
        (
            "x509",
            "-req",
            "-in",
            str(client_request),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(client_cert),
            "-days",
            "1",
            "-sha256",
            "-copy_extensions",
            "copy",
        ),
    )
    for command in commands:
        subprocess.run(
            (openssl_binary, *command),
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    return ca_cert, server_cert, server_key, client_cert, client_key


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        async with asyncio.timeout(15):
            await process.wait()
    except TimeoutError:
        os.killpg(process.pid, signal.SIGKILL)
        async with asyncio.timeout(5):
            await process.wait()
