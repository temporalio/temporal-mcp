from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_METADATA_ENV = "OPS_MCP_ACCEPTANCE_METADATA"


def record(session: int, **values: Any) -> None:
    """Record dynamic fixture values for scenario rendering by the parent runner."""
    raw_path = os.getenv(_METADATA_ENV)
    if not raw_path:
        return
    path = Path(raw_path)
    data: dict[str, Any] = {}
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    data[f"session_{session}"] = values
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
