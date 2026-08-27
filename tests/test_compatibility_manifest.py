import sys
from importlib.metadata import version
from pathlib import Path

import yaml


def test_compatibility_manifest_matches_locked_runtime() -> None:
    manifest = yaml.safe_load(Path("config/compatibility.yaml").read_text(encoding="utf-8"))

    assert manifest["python"]["version"] == ".".join(map(str, sys.version_info[:3]))
    assert manifest["temporal"]["python_sdk_version"] == version("temporalio")
    assert manifest["mcp"]["python_sdk_version"] == version("mcp")
    assert manifest["compatibility_claims"] == {
        "temporal_cloud": False,
        "other_server_versions": False,
    }
