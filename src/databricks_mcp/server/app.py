"""FastAPI application configuration for the Databricks MCP server.

This module sets up the core application and registers all MCP tools.
"""

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastmcp import FastMCP

from ..auth import OAuthTokenProvider
from ..client import DatabricksSQLClient
from ..config import AppConfig, load_config
from ..logging_utils import configure_logging


def _config_path() -> Path:
    """Get the configuration file path from environment or default."""
    path = os.environ.get("DATABRICKS_MCP_CONFIG", "config.example.yml")
    return Path(path)


def _request_id(value: str | None = None) -> str:
    """Generate a unique request ID for tracing."""
    return value or str(uuid.uuid4())


def _register_tools(mcp_server: FastMCP, sql_client: DatabricksSQLClient) -> None:
    """Register all MCP tools with the server.

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
        """Get detailed metadata about a table or view including columns and primary keys."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.table_metadata, catalog, schema, table, rid
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


def create_app(config_path: Path | None = None) -> tuple[FastAPI, DatabricksSQLClient]:
    """Create and configure the FastMCP server application.

    Args:
        config_path: Optional path to config file. If None, uses default from environment.

    Returns:
        tuple: (combined_app, sql_client)
    """
    if config_path is None:
        config_path = _config_path()

    config = load_config(config_path)
    configure_logging(config.observability.log_level)

    token_provider = OAuthTokenProvider(config.oauth)
    sql_client = DatabricksSQLClient(config, token_provider)

    mcp_server = FastMCP(name="databricks-mcp")
    _register_tools(mcp_server, sql_client)

    mcp_app = mcp_server.http_app()

    app = FastAPI(
        title="Databricks MCP Server",
        description="Databricks MCP Server for the Databricks Apps",
        version="0.1.0",
        lifespan=mcp_app.lifespan,
    )

    @app.get("/", include_in_schema=False)
    async def health_check() -> dict:
        """Health check endpoint."""
        return {"message": "Databricks MCP Server is running", "status": "healthy"}

    combined_app = FastAPI(
        title="Databricks MCP App",
        routes=[
            *mcp_app.routes,
            *app.routes,
        ],
        lifespan=mcp_app.lifespan,
    )

    return combined_app, sql_client


combined_app, _sql_client = create_app()
