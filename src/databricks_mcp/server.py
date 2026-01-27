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


def build_app(
    config: AppConfig,
    sql_client: DatabricksSQLClient,
) -> FastMCP:
    app = FastMCP("databricks-mcp")

    @app.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        """List all available catalogs in the Databricks workspace."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_catalogs, rid)

    @app.tool()
    async def list_schemas(catalog: str, request_id: str | None = None) -> list[str]:
        """List all schemas within a specified catalog."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_schemas, catalog, rid)

    @app.tool()
    async def list_tables(
        catalog: str, schema: str, request_id: str | None = None
    ) -> dict[str, Any]:
        """List all tables within a specified catalog and schema."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_tables, catalog, schema, rid)

    @app.tool()
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

    @app.tool()
    async def partition_info(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get partition information and statistics for a table."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.partition_info, catalog, schema, table, rid
        )

    @app.tool()
    async def approx_count(
        catalog: str,
        schema: str,
        table: str,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get approximate row count for a table for audience sizing without returning data."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.approx_count, catalog, schema, table, predicate, rid
        )

    @app.tool()
    async def aggregate_metric(
        catalog: str,
        schema: str,
        table: str,
        metric_type: str,
        metric_column: str,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Calculate aggregated metric (COUNT, SUM, AVG, MIN, MAX) without returning individual rows."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.aggregate_metric, catalog, schema, table, metric_type, metric_column, predicate, rid
        )

    @app.tool()
    async def create_temp_table(
        temp_table_name: str,
        source_query: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a temporary table from a SELECT query combining multiple views or data sources.
        
        This enables aggregating data from multiple tables/views with different business logics
        for lead generation purposes. The temporary table is session-scoped and persists for
        the duration of the AI agent session, allowing subsequent queries on the combined data.
        
        Parameters:
        - temp_table_name: Name for the temporary table (alphanumeric and underscores only)
        - source_query: SELECT query that combines tables/views (all must be in allowlisted catalogs/schemas)
        
        Example:
        temp_table_name: "qualified_leads"
        source_query: "SELECT t1.customer_id, t1.total_purchases, t2.engagement_score 
                       FROM catalog.schema.purchases t1 
                       JOIN catalog.schema.engagement t2 ON t1.customer_id = t2.customer_id 
                       WHERE t1.total_purchases > 1000 AND t2.engagement_score > 0.7"
        
        Returns metadata about the created table including row count.
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.create_temp_table, temp_table_name, source_query, rid
        )


    return app


def main() -> None:
    config = load_config(_config_path())
    configure_logging(config.observability.log_level)
    token_provider = OAuthTokenProvider(config.oauth)
    sql_client = DatabricksSQLClient(config, token_provider)
    app = build_app(config, sql_client)
    app.run()


if __name__ == "__main__":
    main()
