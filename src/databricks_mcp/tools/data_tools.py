"""Data analysis tools for MCP server.

This module contains MCP tools for data operations like querying,
listing catalogs, schemas, tables, and retrieving metadata.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from ..db import DatabricksSQLClient


def _request_id(value: str | None = None) -> str:
    """Generate a unique request ID for tracing."""
    return value or str(uuid.uuid4())


def register_data_tools(mcp_server: Any, sql_client: DatabricksSQLClient) -> None:
    """Register all data analysis MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance
        sql_client: DatabricksSQLClient for database operations
    """

    @mcp_server.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        """List all available catalogs in the Databricks workspace."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_catalogs, rid)

    @mcp_server.tool()
    async def list_schemas(catalog: str, request_id: str | None = None) -> list[str]:
        """List all schemas within a specified catalog."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_schemas, catalog, rid)

    @mcp_server.tool()
    async def list_tables(
        catalog: str, schema: str, request_id: str | None = None
    ) -> dict[str, Any]:
        """List all tables within a specified catalog and schema."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_tables, catalog, schema, rid)

    @mcp_server.tool()
    async def table_metadata(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get detailed metadata about a table or view including columns and primary keys.
        
        This tool merges live Databricks metadata with static metadata from CSV files
        (if configured). Static metadata provides additional business context like
        descriptions, example values, constraints, and ownership information.
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.table_metadata, catalog, schema, table, rid
        )

    @mcp_server.tool()
    async def get_static_metadata(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get static metadata for a table from CSV files.
        
        Returns only the static metadata without querying Databricks.
        Useful for getting business context, column descriptions, and documentation
        without network calls to Databricks.
        
        Returns:
            Dictionary with:
            - enabled: Whether static metadata is configured
            - metadata: List of column metadata (if available)
            - message: Status message
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.get_static_metadata, catalog, schema, table, rid
        )

    @mcp_server.tool()
    async def preview_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Preview query results with a limited number of rows before full execution."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.preview_query, sql, limit, timeout_seconds, rid
        )

    @mcp_server.tool()
    async def run_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a SQL query and return results with configurable row limit and timeout."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.run_query, sql, limit, timeout_seconds, rid
        )
