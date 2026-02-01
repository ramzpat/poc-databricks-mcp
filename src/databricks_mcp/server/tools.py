"""Tools module for the Databricks MCP server.

This module defines all the tools (functions) that the MCP server exposes to clients.
Tools are the core functionality of an MCP server - they are callable functions that
AI assistants and other clients can invoke to perform specific actions.

Each tool should:
- Have a clear, descriptive name
- Include comprehensive docstrings (used by AI to understand when to call the tool)
- Return structured data (typically dict or list)
- Handle errors gracefully
"""

import asyncio
import uuid
from typing import Any

from ..client import DatabricksSQLClient
from ..jobs import DatabricksJobsClient


def _request_id(value: str | None = None) -> str:
    """Generate a unique request ID for tracing."""
    return value or str(uuid.uuid4())


def load_tools(mcp_server: Any, sql_client: DatabricksSQLClient, jobs_client: DatabricksJobsClient) -> None:
    """Register all MCP tools with the server.

    This function is called during server initialization to register all available
    tools with the MCP server instance. Tools are registered using the @mcp_server.tool
    decorator, which makes them available to clients via the MCP protocol.

    Args:
        mcp_server: The FastMCP server instance to register tools with
        sql_client: DatabricksSQLClient for database operations
        jobs_client: DatabricksJobsClient for job operations
    """

    @mcp_server.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        """List all available catalogs in the Databricks workspace.

        Returns:
            list[str]: List of catalog names
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_catalogs, rid)

    @mcp_server.tool()
    async def list_schemas(catalog: str, request_id: str | None = None) -> list[str]:
        """List all schemas within a specified catalog.

        Args:
            catalog: The catalog name
            request_id: Optional request ID for tracing

        Returns:
            list[str]: List of schema names in the catalog
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_schemas, catalog, rid)

    @mcp_server.tool()
    async def list_tables(
        catalog: str, schema: str, request_id: str | None = None
    ) -> dict[str, Any]:
        """List all tables within a specified catalog and schema.

        Args:
            catalog: The catalog name
            schema: The schema name
            request_id: Optional request ID for tracing

        Returns:
            dict: Information about tables in the schema
        """
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

        Args:
            catalog: The catalog name
            schema: The schema name
            table: The table name
            request_id: Optional request ID for tracing

        Returns:
            dict: Table metadata including columns, types, and constraints
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.table_metadata, catalog, schema, table, rid
        )

    @mcp_server.tool()
    async def partition_info(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get partition information and statistics for a table.

        Args:
            catalog: The catalog name
            schema: The schema name
            table: The table name
            request_id: Optional request ID for tracing

        Returns:
            dict: Partition information and table statistics
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.partition_info, catalog, schema, table, rid
        )

    @mcp_server.tool()
    async def sample_data(
        catalog: str,
        schema: str,
        table: str,
        limit: int | None = None,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve sample data from a table with optional filtering and row limit.

        Args:
            catalog: The catalog name
            schema: The schema name
            table: The table name
            limit: Maximum number of rows to return
            predicate: Optional WHERE clause for filtering
            request_id: Optional request ID for tracing

        Returns:
            dict: Sample data from the table
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.sample_data, catalog, schema, table, limit, predicate, rid
        )

    @mcp_server.tool()
    async def preview_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Preview query results with a limited number of rows before full execution.

        Do not start the SQL with a comment; guardrails block comment-first statements.

        Args:
            sql: SQL query to preview
            limit: Maximum number of rows to return
            timeout_seconds: Query timeout in seconds
            request_id: Optional request ID for tracing

        Returns:
            dict: Query preview results
        """
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
        """Execute a SQL query and return results with configurable row limit and timeout.

        Do not start the SQL with a comment; guardrails block comment-first statements.

        Args:
            sql: SQL query to execute
            limit: Maximum number of rows to return
            timeout_seconds: Query timeout in seconds
            request_id: Optional request ID for tracing

        Returns:
            dict: Query results
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.run_query, sql, limit, timeout_seconds, rid
        )
