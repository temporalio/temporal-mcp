from pathlib import Path

import pytest

from ops_mcp.config import AuthorizationError, ConfigurationError, TargetRegistry
from ops_mcp.models import Principal


def write_config(path: Path) -> None:
    path.write_text(
        """
groups:
  support:
    targets:
      orders-prod:
        aliases: [orders prod]
        account: acme
        cluster: primary
        namespace: orders
        address: temporal.example:7233
        tls: true
  engineering:
    targets:
      orders-dev:
        aliases: [orders dev]
        account: acme
        cluster: dev
        namespace: orders-dev
        address: localhost:7233
""",
        encoding="utf-8",
    )


def test_resolve_and_authorize_are_group_scoped(tmp_path: Path) -> None:
    path = tmp_path / "targets.yaml"
    write_config(path)
    registry = TargetRegistry.load(path)
    support = Principal(subject="alice", groups=("support",))

    target = registry.resolve(support, "Orders_Prod")

    assert target.id == "orders-prod"
    assert target.namespace == "orders"
    assert registry.authorize(support, target.id) == target
    with pytest.raises(AuthorizationError):
        registry.resolve(support, "orders dev")
    with pytest.raises(AuthorizationError):
        registry.authorize(support, "orders-dev")


def test_rejects_config_without_groups(tmp_path: Path) -> None:
    path = tmp_path / "targets.yaml"
    path.write_text("groups: {}\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="non-empty"):
        TargetRegistry.load(path)
