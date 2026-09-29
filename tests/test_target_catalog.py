from pathlib import Path

import pytest

from ops_mcp.runtime import (
    InvalidConfigurationError,
    Principal,
    TargetNotAuthorizedError,
    TargetNotFoundError,
)
from ops_mcp.targets import YamlTargetCatalog
from ops_mcp.targets.models import ConnectionDescriptor


def test_customer_example_config_is_valid() -> None:
    catalog = YamlTargetCatalog.load("config/targets.example.yaml")
    principal = Principal(subject="operator", groups=("operators",))

    target = catalog.resolve(principal, "my cloud target")

    assert target.target_ref == "my-cloud-target"
    assert target.account is None
    assert target.cluster is None
    assert target.connection.tls is True
    assert target.connection.api_key_env == "TEMPORAL_API_KEY"


def test_catalog_loads_demo_fixture_yaml() -> None:
    catalog = YamlTargetCatalog.load("config/targets.demo.yaml")
    principal = Principal(subject="demo", groups=("demo-support", "demo-platform"))

    target = catalog.resolve(principal, "Orders_Prod")

    assert target.target_ref == "orders-prod"
    assert target.namespace == "default"
    assert target.connection.address == "127.0.0.1:7233"
    assert catalog.authorize(principal, target.target_ref) == target


def test_catalog_loads_mtls_paths(tmp_path: Path) -> None:
    config = tmp_path / "targets.yaml"
    config.write_text(
        """
groups:
  support:
    targets:
      orders-prod:
        account: acme
        cluster: prod
        namespace: orders
        address: temporal.example:7233
        tls: true
        server_name: temporal.internal
        client_cert_path: /run/secrets/temporal-client.pem
        client_key_path: /run/secrets/temporal-client.key
        server_ca_cert_path: /run/secrets/temporal-ca.pem
""",
        encoding="utf-8",
    )

    target = YamlTargetCatalog.load(config).resolve(
        Principal(subject="operator", groups=("support",)), "orders-prod"
    )

    assert target.connection.client_cert_path == "/run/secrets/temporal-client.pem"
    assert target.connection.client_key_path == "/run/secrets/temporal-client.key"
    assert target.connection.server_ca_cert_path == "/run/secrets/temporal-ca.pem"


@pytest.mark.parametrize(
    "kwargs",
    (
        {"tls": True, "client_cert_path": "/cert.pem"},
        {"tls": True, "client_key_path": "/key.pem"},
        {"tls": False, "client_cert_path": "/cert.pem", "client_key_path": "/key.pem"},
        {"tls": False, "server_ca_cert_path": "/ca.pem"},
    ),
)
def test_connection_descriptor_rejects_incomplete_or_disabled_mtls(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        ConnectionDescriptor(address="temporal.example:7233", namespace="orders", **kwargs)


def test_catalog_does_not_distinguish_missing_from_unauthorized(tmp_path: Path) -> None:
    config = tmp_path / "targets.yaml"
    config.write_text(
        """
groups:
  support:
    targets:
      orders-prod:
        aliases: [orders prod]
        account: acme
        cluster: prod
        namespace: orders
        address: temporal.example:7233
        tls: true
""",
        encoding="utf-8",
    )
    catalog = YamlTargetCatalog.load(config)
    principal = Principal(subject="developer", groups=("development",))

    with pytest.raises(TargetNotFoundError) as unauthorized:
        catalog.resolve(principal, "orders prod")
    with pytest.raises(TargetNotFoundError) as missing:
        catalog.resolve(principal, "payments prod")
    assert unauthorized.value.code == missing.value.code
    with pytest.raises(TargetNotAuthorizedError):
        catalog.authorize(principal, "orders-prod")


@pytest.mark.parametrize(
    "invalid_field",
    (
        'tls: "false"',
        "aliases: orders-prod",
        "address:",
        "credential_generation: true",
        "unexpected: value",
    ),
)
def test_catalog_rejects_coercible_or_unknown_values(tmp_path: Path, invalid_field: str) -> None:
    config = tmp_path / "targets.yaml"
    config.write_text(
        f"""
groups:
  support:
    targets:
      orders-prod:
        account: acme
        cluster: prod
        namespace: orders
        address: temporal.example:7233
        {invalid_field}
""",
        encoding="utf-8",
    )

    with pytest.raises(InvalidConfigurationError, match="orders-prod"):
        YamlTargetCatalog.load(config)


@pytest.mark.parametrize("document", ("hello", "42", "[]"))
def test_catalog_rejects_non_mapping_root(tmp_path: Path, document: str) -> None:
    config = tmp_path / "targets.yaml"
    config.write_text(document, encoding="utf-8")

    with pytest.raises(InvalidConfigurationError, match="root must be a mapping"):
        YamlTargetCatalog.load(config)
