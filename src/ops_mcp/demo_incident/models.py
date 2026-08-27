from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IncidentWorkItem:
    team: str
    domain: str
    mode: str
