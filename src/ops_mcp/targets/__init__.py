"""Target resolution and authorization."""

from ops_mcp.targets.catalog import TargetCatalog, YamlTargetCatalog
from ops_mcp.targets.models import ConnectionDescriptor, ResolvedTarget

__all__ = ["ConnectionDescriptor", "ResolvedTarget", "TargetCatalog", "YamlTargetCatalog"]
