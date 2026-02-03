"""Databricks MCP server package."""

# Note: Modules are available via submodule imports to avoid circular dependencies
# and module-level initialization issues:
# from databricks_mcp.auth import OAuthTokenProvider
# from databricks_mcp.db import DatabricksSQLClient
# from databricks_mcp.guardrails import sanitize_identifier
# from databricks_mcp.tools import register_data_tools
# from databricks_mcp.server import main

# Export module names for `from databricks_mcp import *` compatibility
__all__ = [
    "config",
    "server",
]
