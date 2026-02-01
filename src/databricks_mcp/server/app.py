"""FastAPI application configuration for the Databricks MCP server.

This module sets up the core application by:
1. Creating and configuring the FastMCP server instance
2. Loading and registering all MCP tools
3. Setting up CORS middleware for cross-origin requests
4. Combining MCP routes with standard FastAPI routes
5. Optionally serving static files for a web frontend

The MCP (Model Context Protocol) server provides tools that can be called by
AI assistants and other clients. FastMCP makes it easy to expose these tools
over HTTP using the MCP protocol standard.
"""

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastmcp import FastMCP

from ..auth import OAuthTokenProvider
from ..client import DatabricksSQLClient
from ..config import AppConfig, load_config
from ..jobs import DatabricksJobsClient
from ..logging_utils import configure_logging
from .tools import load_tools
from .utils import header_store


def _config_path() -> Path:
    """Get the configuration file path from environment or default."""
    path = os.environ.get("DATABRICKS_MCP_CONFIG", "config.example.yml")
    return Path(path)


def create_app(config_path: Path | None = None) -> tuple[FastAPI, DatabricksSQLClient, DatabricksJobsClient]:
    """Create and configure the FastMCP server application.

    Args:
        config_path: Optional path to config file. If None, uses default from environment.

    Returns:
        tuple: (combined_app, sql_client, jobs_client)
    """
    if config_path is None:
        config_path = _config_path()

    config = load_config(config_path)
    configure_logging(config.observability.log_level)

    token_provider = OAuthTokenProvider(config.oauth)
    sql_client = DatabricksSQLClient(config, token_provider)
    jobs_client = DatabricksJobsClient(config, token_provider)

    # Create FastMCP server instance
    mcp_server = FastMCP(name="databricks-mcp")

    # Load and register all tools with the MCP server
    load_tools(mcp_server, sql_client, jobs_client)

    # Convert the MCP server to a streamable HTTP application
    mcp_app = mcp_server.http_app()

    # Create a separate FastAPI instance for additional API endpoints
    app = FastAPI(
        title="Databricks MCP Server",
        description="Databricks MCP Server for the Databricks Apps",
        version="0.1.0",
        lifespan=mcp_app.lifespan,
    )

    @app.get("/", include_in_schema=False)
    async def serve_index() -> dict:
        """Serve the index page or health check."""
        static_dir = Path(__file__).parent.parent.parent / "static"
        if static_dir.exists() and (static_dir / "index.html").exists():
            return FileResponse(static_dir / "index.html")
        else:
            return {"message": "Databricks MCP Server is running", "status": "healthy"}

    # Create the final application by combining MCP routes with custom API routes
    combined_app = FastAPI(
        title="Databricks MCP App",
        routes=[
            *mcp_app.routes,
            *app.routes,
        ],
        lifespan=mcp_app.lifespan,
    )

    # Middleware to capture the user token from the request headers
    @combined_app.middleware("http")
    async def capture_headers(request: Request, call_next):
        """Middleware to capture request headers for authentication."""
        header_store.set(dict(request.headers))
        return await call_next(request)

    return combined_app, sql_client, jobs_client


# Create the app instance
combined_app, _sql_client, _jobs_client = create_app()
