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
    async def create_temp_view(
        temp_table_name: str,
        source_tables: list[dict[str, str]],
        columns: list[dict[str, str]],
        join_conditions: list[dict[str, str]] | None = None,
        where_conditions: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a temporary table by combining multiple views or data sources with structured parameters.
        
        IMPORTANT: Temporary tables are session-scoped and will be automatically deleted when the 
        Databricks session ends. They CANNOT be accessed from other AI agent sessions or persist beyond 
        the current session.
        
        This tool prevents arbitrary SQL queries by requiring structured parameters. All source tables 
        must be in allowlisted catalogs/schemas.
        
        Parameters:
        - temp_table_name: Name for the temporary table (alphanumeric and underscores only)
        - source_tables: List of source tables, each with:
            * catalog: Catalog name (must be allowlisted)
            * schema: Schema name (must be allowlisted)  
            * table: Table or view name
            * alias: Short alias for the table (e.g., "t1", "customers")
        - columns: List of columns to include, each with:
            * table_alias: Alias of the source table
            * column: Column name from that table
            * alias (optional): New name for the column in results
        - join_conditions (optional): List of JOINs, each with:
            * type: "INNER", "LEFT", "RIGHT", or "FULL" (default: "INNER")
            * left_table: Alias of left table
            * left_column: Column from left table
            * right_table: Alias of right table  
            * right_column: Column from right table
        - where_conditions (optional): WHERE clause conditions (without the WHERE keyword)
        
        Example:
        source_tables: [
            {"catalog": "main", "schema": "sales", "table": "purchases", "alias": "p"},
            {"catalog": "main", "schema": "analytics", "table": "engagement", "alias": "e"}
        ]
        columns: [
            {"table_alias": "p", "column": "customer_id"},
            {"table_alias": "p", "column": "total_purchases", "alias": "total_spent"},
            {"table_alias": "e", "column": "engagement_score"}
        ]
        join_conditions: [
            {"type": "INNER", "left_table": "p", "left_column": "customer_id", 
             "right_table": "e", "right_column": "customer_id"}
        ]
        where_conditions: "p.total_purchases > 1000 AND e.engagement_score > 0.7"
        
        Returns metadata including temp table name, row count, and a reminder 
        that the table will be automatically deleted at session end.
        """
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.create_temp_view, temp_table_name, source_tables, columns, 
            join_conditions, where_conditions, rid
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
