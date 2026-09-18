"""Group-scoped target catalog backed by the prototype YAML format."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import yaml

from ops_mcp.runtime.context import Principal
from ops_mcp.runtime.errors import (
    InvalidConfigurationError,
    TargetNotAuthorizedError,
    TargetNotFoundError,
)
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget


def _normalize(value: str) -> str:
    return " ".join(value.casefold().replace("_", " ").replace("-", " ").split())


class TargetCatalog(Protocol):
    def list_authorized(self, principal: Principal) -> tuple[ResolvedTarget, ...]: ...

    def match_authorized(
        self, principal: Principal, human_name: str
    ) -> tuple[ResolvedTarget, ...]: ...

    def resolve(self, principal: Principal, human_name: str) -> ResolvedTarget: ...

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget: ...


class YamlTargetCatalog:
    """Loads the existing `groups.<group>.targets` demo configuration unchanged."""

    def __init__(self, groups: dict[str, dict[str, ResolvedTarget]]) -> None:
        self._groups = groups

    @classmethod
    def load(cls, path: str | Path) -> YamlTargetCatalog:
        config_path = Path(path)
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            raw = {} if loaded is None else loaded
        except (OSError, yaml.YAMLError) as exc:
            raise InvalidConfigurationError(
                f"Unable to read target configuration {config_path}"
            ) from exc

        if not isinstance(raw, dict):
            raise InvalidConfigurationError("Target configuration root must be a mapping")
        raw_groups = raw.get("groups")
        if not isinstance(raw_groups, dict) or not raw_groups:
            raise InvalidConfigurationError(
                "Target configuration must contain a non-empty 'groups' mapping"
            )

        groups: dict[str, dict[str, ResolvedTarget]] = {}
        for group_name, group_value in raw_groups.items():
            if not isinstance(group_name, str) or not group_name.strip():
                raise InvalidConfigurationError("Target group names must be non-empty strings")
            if not isinstance(group_value, dict) or not isinstance(
                group_value.get("targets"), dict
            ):
                raise InvalidConfigurationError(
                    f"Group {group_name!r} must contain a 'targets' mapping"
                )
            targets: dict[str, ResolvedTarget] = {}
            for target_ref, value in group_value["targets"].items():
                if not isinstance(target_ref, str) or not target_ref.strip():
                    raise InvalidConfigurationError(
                        f"Group {group_name!r} contains an invalid target reference"
                    )
                targets[target_ref] = _parse_target(target_ref, value)
            groups[group_name] = targets
        return cls(groups)

    def resolve(self, principal: Principal, human_name: str) -> ResolvedTarget:
        matches = self.match_authorized(principal, human_name)
        if not matches:
            raise TargetNotFoundError(human_name)
        if len(matches) > 1:
            raise InvalidConfigurationError(f"Target name {human_name!r} is ambiguous")
        return matches[0]

    def list_authorized(self, principal: Principal) -> tuple[ResolvedTarget, ...]:
        """Return unique caller-visible destinations in deterministic public order."""
        visible = sorted(
            (
                target
                for group in principal.groups
                for target in self._groups.get(group, {}).values()
            ),
            key=_dedupe_choice_key,
        )
        targets: dict[tuple[str, str, str, str, ConnectionDescriptor], ResolvedTarget] = {}
        for target in visible:
            targets.setdefault(_target_key(target), target)
        return tuple(sorted(targets.values(), key=_target_sort_key))

    def match_authorized(self, principal: Principal, human_name: str) -> tuple[ResolvedTarget, ...]:
        """Return normalized exact matches without inspecting unauthorized groups."""
        wanted = _normalize(human_name)
        matches = {
            _target_key(target): target
            for target in self.list_authorized(principal)
            if wanted in {_normalize(target.target_ref), *map(_normalize, target.aliases)}
        }
        return tuple(sorted(matches.values(), key=_target_sort_key))

    def authorize(self, principal: Principal, target_ref: str) -> ResolvedTarget:
        matches = {
            _target_key(target): target
            for group in principal.groups
            if (target := self._groups.get(group, {}).get(target_ref)) is not None
        }
        if not matches:
            raise TargetNotAuthorizedError(target_ref)
        if len(matches) > 1:
            raise InvalidConfigurationError(
                f"Target reference {target_ref!r} maps to multiple destinations for the caller"
            )
        return next(iter(matches.values()))


def _parse_target(target_ref: str, value: Any) -> ResolvedTarget:
    if not isinstance(value, dict):
        raise InvalidConfigurationError(f"Target {target_ref!r} must be a mapping")
    try:
        unknown = set(value) - {
            "aliases",
            "account",
            "cluster",
            "namespace",
            "address",
            "tls",
            "api_key_env",
            "server_name",
            "credential_generation",
        }
        if unknown:
            raise ValueError(f"unknown fields: {', '.join(sorted(map(str, unknown)))}")
        namespace = _required_string(value, "namespace")
        aliases_value = value.get("aliases", [])
        if not isinstance(aliases_value, list):
            raise ValueError("aliases must be a list")
        aliases = tuple(_non_empty_string(alias, "alias") for alias in aliases_value)
        if len({_normalize(alias) for alias in aliases}) != len(aliases):
            raise ValueError("aliases must be unique")
        tls = value.get("tls", False)
        if not isinstance(tls, bool):
            raise ValueError("tls must be a boolean")
        credential_generation = value.get("credential_generation", 0)
        if isinstance(credential_generation, bool) or not isinstance(credential_generation, int):
            raise ValueError("credential_generation must be an integer")
        connection = ConnectionDescriptor(
            address=_required_string(value, "address"),
            namespace=namespace,
            tls=tls,
            api_key_env=_optional_string(value.get("api_key_env")),
            server_name=_optional_string(value.get("server_name")),
            credential_generation=credential_generation,
        )
        return ResolvedTarget(
            target_ref=target_ref,
            aliases=aliases,
            account=_required_string(value, "account"),
            cluster=_required_string(value, "cluster"),
            namespace=namespace,
            connection=connection,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidConfigurationError(f"Target {target_ref!r} is invalid") from exc


def _optional_string(value: Any) -> str | None:
    return _non_empty_string(value, "optional string") if value is not None else None


def _required_string(value: dict[str, Any], name: str) -> str:
    try:
        return _non_empty_string(value[name], name)
    except KeyError:
        raise ValueError(f"{name} is required") from None


def _non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _target_key(
    target: ResolvedTarget,
) -> tuple[str, str, str, str, ConnectionDescriptor]:
    return (
        target.target_ref,
        target.account,
        target.cluster,
        target.namespace,
        target.connection,
    )


def _target_sort_key(target: ResolvedTarget) -> tuple[str, str, str, str, str]:
    return (
        _normalize(target.target_ref),
        _normalize(target.aliases[0] if target.aliases else target.target_ref),
        _normalize(target.account),
        _normalize(target.cluster),
        _normalize(target.namespace),
    )


def _dedupe_choice_key(target: ResolvedTarget) -> tuple[object, ...]:
    return (*_target_sort_key(target), tuple(map(_normalize, target.aliases)))
