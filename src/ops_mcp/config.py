from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from ops_mcp.models import Principal, Target


class ConfigurationError(RuntimeError):
    pass


class AuthorizationError(RuntimeError):
    pass


def _normalize(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").replace("-", " ").split())


class TargetRegistry:
    def __init__(self, groups: dict[str, dict[str, Target]]) -> None:
        self._groups = groups

    @classmethod
    def load(cls, path: str | Path) -> TargetRegistry:
        config_path = Path(path)
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigurationError(f"Unable to read target config {config_path}: {exc}") from exc

        raw_groups = raw.get("groups")
        if not isinstance(raw_groups, dict) or not raw_groups:
            raise ConfigurationError("Target config must contain a non-empty 'groups' mapping")

        groups: dict[str, dict[str, Target]] = {}
        for group_name, group_value in raw_groups.items():
            if not isinstance(group_value, dict) or not isinstance(
                group_value.get("targets"), dict
            ):
                raise ConfigurationError(f"Group {group_name!r} must contain a 'targets' mapping")
            targets: dict[str, Target] = {}
            for target_id, value in group_value["targets"].items():
                if not isinstance(value, dict):
                    raise ConfigurationError(f"Target {target_id!r} must be a mapping")
                try:
                    target = Target(
                        id=str(target_id),
                        aliases=tuple(str(alias) for alias in value.get("aliases", [])),
                        account=str(value["account"]),
                        cluster=str(value["cluster"]),
                        namespace=str(value["namespace"]),
                        address=str(value["address"]),
                        tls=bool(value.get("tls", False)),
                        api_key_env=_optional_string(value.get("api_key_env")),
                        server_name=_optional_string(value.get("server_name")),
                    )
                except KeyError as exc:
                    raise ConfigurationError(
                        f"Target {target_id!r} is missing required field {exc.args[0]!r}"
                    ) from exc
                targets[target.id] = target
            groups[str(group_name)] = targets
        return cls(groups)

    def resolve(self, principal: Principal, human_name: str) -> Target:
        wanted = _normalize(human_name)
        matches: dict[tuple[str, str, str, str], Target] = {}
        for group in principal.groups:
            for target in self._groups.get(group, {}).values():
                names = (target.id, *target.aliases)
                if wanted in {_normalize(name) for name in names}:
                    key = (target.id, target.account, target.cluster, target.namespace)
                    matches[key] = target
        if not matches:
            raise AuthorizationError(
                f"No target named {human_name!r} is configured for the caller's groups"
            )
        if len(matches) > 1:
            choices = ", ".join(sorted(f"{t.account}/{t.namespace}" for t in matches.values()))
            raise ConfigurationError(f"Target name {human_name!r} is ambiguous: {choices}")
        return next(iter(matches.values()))

    def authorize(self, principal: Principal, target_id: str) -> Target:
        matches: dict[tuple[str, str, str, str], Target] = {}
        for group in principal.groups:
            target = self._groups.get(group, {}).get(target_id)
            if target:
                key = (target.id, target.account, target.cluster, target.namespace)
                matches[key] = target
        if not matches:
            raise AuthorizationError(
                f"Target {target_id!r} is not configured for the caller's groups; resolve it first"
            )
        if len(matches) > 1:
            raise ConfigurationError(
                f"Target id {target_id!r} maps to multiple destinations for this caller"
            )
        return next(iter(matches.values()))


def demo_principal() -> Principal:
    group = os.getenv("OPS_MCP_DEMO_GROUP", "demo-support")
    return Principal(subject="unauthenticated-demo", groups=(group,))


def default_config_path() -> Path:
    configured = os.getenv("OPS_MCP_TARGET_CONFIG")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "config" / "targets.example.yaml"


def _optional_string(value: Any) -> str | None:
    return str(value) if value is not None else None
