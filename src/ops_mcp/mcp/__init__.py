"""Generalizable MCP composition root and read-only adapters."""

from ops_mcp.mcp.app import create_mcp, main
from ops_mcp.mcp.composition import (
    CompositionRoot,
    ServiceBundle,
    build_composition_root,
)

__all__ = [
    "CompositionRoot",
    "ServiceBundle",
    "build_composition_root",
    "create_mcp",
    "main",
]
