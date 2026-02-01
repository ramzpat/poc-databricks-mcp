"""FastAPI application configuration and entry point for the Databricks MCP server.

This module sets up the core application and registers all MCP tools.
"""

import argparse
import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastmcp import FastMCP

from .auth import OAuthTokenProvider
from .config import load_config
from .db import DatabricksSQLClient
from .logging_utils import configure_logging
from .tools import register_data_tools, register_user_tools


def _config_path() -> Path:
    """Get the configuration file path from environment or default."""
    path = os.environ.get("DATABRICKS_MCP_CONFIG", "config.example.yml")
    return Path(path)


def _register_tools(mcp_server: FastMCP, sql_client: DatabricksSQLClient) -> None:
    """Register all MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance
        sql_client: DatabricksSQLClient for database operations
    """
    register_data_tools(mcp_server, sql_client)
    register_user_tools(mcp_server)


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


def main() -> None:
    """Start the MCP server using uvicorn.

    This function is the main entry point for the application. It configures and
    starts the uvicorn ASGI server with the combined FastAPI/FastMCP application.

    Configuration:
        - host: "0.0.0.0" - Binds to all network interfaces
        - port: Configurable via --port argument (default: 8000)
    """
    parser = argparse.ArgumentParser(description="Start the Databricks MCP server")
    parser.add_argument(
        "--port", type=int, default=8000, help="Port to run the server on (default: 8000)"
    )
    args = parser.parse_args()

    uvicorn.run(
        "databricks_mcp.server:combined_app",
        host="0.0.0.0",
        port=args.port,
    )


combined_app, _sql_client = create_app()
