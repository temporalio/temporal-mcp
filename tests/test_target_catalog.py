from pathlib import Path

import pytest

from ops_mcp.runtime import Principal, TargetNotAuthorizedError, TargetNotFoundError
from ops_mcp.targets import YamlTargetCatalog


def test_new_catalog_adapts_existing_demo_yaml() -> None:
    catalog = YamlTargetCatalog.load("config/targets.example.yaml")
    principal = Principal(subject="demo", groups=("demo-support", "demo-platform"))

    target = catalog.resolve(principal, "Orders_Prod")

    assert target.target_ref == "orders-prod"
    assert target.namespace == "default"
    assert target.connection.address == "127.0.0.1:7233"
    assert catalog.authorize(principal, target.target_ref) == target


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
