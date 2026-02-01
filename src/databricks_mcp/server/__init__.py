"""MCP server entry point and initialization."""

import argparse

import uvicorn


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
        "databricks_mcp.server.app:combined_app",
        host="0.0.0.0",
        port=args.port,
    )
