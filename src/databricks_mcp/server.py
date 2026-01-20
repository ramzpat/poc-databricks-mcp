from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from .auth import OAuthTokenProvider
from .client import DatabricksSQLClient
from .config import AppConfig, load_config
from .logging_utils import configure_logging


def _request_id(value: str | None = None) -> str:
    return value or str(uuid.uuid4())


def _config_path() -> Path:
    path = os.environ.get("DATABRICKS_MCP_CONFIG", "config.example.yml")
    return Path(path)


def build_app(config: AppConfig, client: DatabricksSQLClient) -> FastMCP:
    app = FastMCP("databricks-mcp")

    @app.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.list_catalogs, rid)

    @app.tool()
    async def list_schemas(catalog: str, request_id: str | None = None) -> list[str]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.list_schemas, catalog, rid)

    @app.tool()
    async def list_tables(catalog: str, schema: str, request_id: str | None = None) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.list_tables, catalog, schema, rid)

    @app.tool()
    async def table_metadata(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.table_metadata, catalog, schema, table, rid)

    @app.tool()
    async def partition_info(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.partition_info, catalog, schema, table, rid)

    @app.tool()
    async def sample_data(
        catalog: str,
        schema: str,
        table: str,
        limit: int | None = None,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.sample_data, catalog, schema, table, limit, predicate, rid)

    @app.tool()
    async def preview_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.preview_query, sql, limit, timeout_seconds, rid)

    @app.tool()
    async def run_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(client.run_query, sql, limit, timeout_seconds, rid)

    @app.tool()
    async def health_check() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    config = load_config(_config_path())
    configure_logging(config.observability.log_level)
    token_provider = OAuthTokenProvider(config.oauth)
    client = DatabricksSQLClient(config, token_provider)
    app = build_app(config, client)
    app.run()


if __name__ == "__main__":
    main()
