"""Generalizable MCP composition root and direct-read adapters."""

from ops_mcp.mcp.app import create_mcp, main, mcp
from ops_mcp.mcp.composition import (
    CompositionRoot,
    ReasoningServiceSlots,
    ServiceBundle,
    build_composition_root,
)

__all__ = [
    "CompositionRoot",
    "ReasoningServiceSlots",
    "ServiceBundle",
    "build_composition_root",
    "create_mcp",
    "main",
    "mcp",
]
