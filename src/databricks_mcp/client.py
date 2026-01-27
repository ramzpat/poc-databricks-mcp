from __future__ import annotations

import logging
import uuid
from threading import Semaphore
from typing import Any, Iterable

import databricks.sql
from databricks.sql.exc import Error as DatabricksError

from .auth import OAuthTokenProvider
from .config import AppConfig
from .errors import GuardrailError, QueryError
from .guardrails import (
    clamp_limit,
    detect_statement_type,
    effective_timeout,
    ensure_catalog_allowed,
    ensure_schema_allowed,
    ensure_statement_allowed,
    sanitize_identifier,
    validate_temp_table_query,
)
from .logging_utils import log_extra


class DatabricksSQLClient:
    def __init__(self, config: AppConfig, token_provider: OAuthTokenProvider) -> None:
        self._config = config
        self._token_provider = token_provider
        self._log = logging.getLogger(__name__)
        self._semaphore = Semaphore(config.limits.max_concurrent_queries)

    def list_catalogs(self, request_id: str | None = None) -> list[str]:
        return list(self._config.scopes.catalogs)

    def list_schemas(self, catalog: str, request_id: str | None = None) -> list[str]:
        ensure_catalog_allowed(catalog, self._config.scopes)
        return list(self._config.scopes.catalogs.get(catalog, []))

    def list_tables(
        self, catalog: str, schema: str, request_id: str | None = None
    ) -> dict[str, Any]:
        ensure_catalog_allowed(catalog, self._config.scopes)
        ensure_schema_allowed(catalog, schema, self._config.scopes)
        sql = (
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM system.information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ?"
        )
        rows, truncated = self._execute(
            sql, (catalog, schema), limit=None, request_id=request_id
        )
        return {
            "tables": [
                {
                    "catalog": row.get("table_catalog"),
                    "schema": row.get("table_schema"),
                    "name": row.get("table_name"),
                    "type": row.get("table_type"),
                }
                for row in rows
            ],
            "truncated": truncated,
        }

    def table_metadata(
        self, catalog: str, schema: str, table: str, request_id: str | None = None
    ) -> dict[str, Any]:
        ensure_catalog_allowed(catalog, self._config.scopes)
        ensure_schema_allowed(catalog, schema, self._config.scopes)
        safe_table = sanitize_identifier(table, "table")
        table_type_sql = (
            "SELECT table_type "
            "FROM system.information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?"
        )
        columns_sql = (
            "SELECT column_name, data_type, is_nullable, comment, ordinal_position "
            "FROM system.information_schema.columns "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position"
        )
        pk_sql = (
            "SELECT kcu.column_name "
            "FROM system.information_schema.table_constraints tc "
            "JOIN system.information_schema.key_column_usage kcu "
            "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            "WHERE tc.table_catalog = ? AND tc.table_schema = ? AND tc.table_name = ? "
            "AND tc.constraint_type = 'PRIMARY KEY' "
            "ORDER BY kcu.ordinal_position"
        )
        detail_sql = f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{safe_table}`"

        table_type_rows, _ = self._execute(
            table_type_sql,
            (catalog, schema, safe_table),
            limit=None,
            request_id=request_id,
        )
        table_type = (
            table_type_rows[0].get("table_type") if table_type_rows else None
        )
        is_view = (table_type or "").upper() == "VIEW"

        view_definition = None
        if is_view:
            view_sql = (
                "SELECT view_definition "
                "FROM system.information_schema.views "
                "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?"
            )
            view_rows, _ = self._execute(
                view_sql,
                (catalog, schema, safe_table),
                limit=None,
                request_id=request_id,
            )
            view_definition = (
                view_rows[0].get("view_definition") if view_rows else None
            )

        columns, _ = self._execute(
            columns_sql,
            (catalog, schema, safe_table),
            limit=None,
            request_id=request_id,
        )
        primary_keys, _ = self._execute(
            pk_sql, (catalog, schema, safe_table), limit=None, request_id=request_id
        )
        detail = {}
        if not is_view:
            details, _ = self._execute(
                detail_sql, params=None, limit=None, request_id=request_id
            )
            detail = details[0] if details else {}

        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "table_type": table_type,
            "columns": [
                {
                    "name": col.get("column_name"),
                    "data_type": col.get("data_type"),
                    "nullable": col.get("is_nullable"),
                    "comment": col.get("comment"),
                    "ordinal_position": col.get("ordinal_position"),
                }
                for col in columns
            ],
            "primary_keys": [pk.get("column_name") for pk in primary_keys],
            "partition_columns": detail.get("partitionColumns") or [],
            "row_count": detail.get("numRows"),
            "view_definition": view_definition,
        }

    def partition_info(
        self, catalog: str, schema: str, table: str, request_id: str | None = None
    ) -> dict[str, Any]:
        ensure_catalog_allowed(catalog, self._config.scopes)
        ensure_schema_allowed(catalog, schema, self._config.scopes)
        safe_table = sanitize_identifier(table, "table")
        sql = f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{safe_table}`"
        rows, _ = self._execute(sql, params=None, limit=None, request_id=request_id)
        detail = rows[0] if rows else {}
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "partition_columns": detail.get("partitionColumns") or [],
            "statistics": {
                "row_count": detail.get("numRows"),
                "size_in_bytes": detail.get("sizeInBytes"),
            },
        }

    def approx_count(
        self,
        catalog: str,
        schema: str,
        table: str,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get approximate row count for a table with optional filtering.

        Does not return any actual data, only aggregated count for audience sizing.
        Useful for lead generation to estimate audience based on conditions.

        Parameters:
        catalog (str): The catalog name
        schema (str): The schema name
        table (str): The table name
        predicate (str | None): Optional WHERE clause predicate for filtering
        request_id (str | None): Request tracking ID

        Returns:
        dict[str, Any]: Catalog, schema, table, approximate count, and conditions applied

        Raises:
        GuardrailError: If guardrails fail
        """
        ensure_catalog_allowed(catalog, self._config.scopes)
        ensure_schema_allowed(catalog, schema, self._config.scopes)
        safe_table = sanitize_identifier(table, "table")

        predicate_clause = f" WHERE {predicate}" if predicate else ""
        sql = f"SELECT COUNT(*) as approx_count FROM `{catalog}`.`{schema}`.`{safe_table}`{predicate_clause}"
        timeout_value = effective_timeout(None, self._config.limits)
        rows, _ = self._execute(
            sql,
            params=None,
            limit=None,
            timeout=timeout_value,
            request_id=request_id,
        )
        count = rows[0].get("approx_count", 0) if rows else 0
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "approx_count": count,
            "predicate": predicate,
        }

    def aggregate_metric(
        self,
        catalog: str,
        schema: str,
        table: str,
        metric_type: str,
        metric_column: str,
        predicate: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Calculate aggregated metric on a table without returning individual rows.

        Supports COUNT, SUM, AVG, MIN, MAX aggregations for audience sizing.
        Does not return any actual data, only the aggregated result.

        Parameters:
        catalog (str): The catalog name
        schema (str): The schema name
        table (str): The table name
        metric_type (str): One of COUNT, SUM, AVG, MIN, MAX
        metric_column (str): Column name to aggregate (for COUNT, can be "*")
        predicate (str | None): Optional WHERE clause predicate for filtering
        request_id (str | None): Request tracking ID

        Returns:
        dict[str, Any]: Aggregated metric result

        Raises:
        GuardrailError: If metric_type or column is invalid
        """
        ensure_catalog_allowed(catalog, self._config.scopes)
        ensure_schema_allowed(catalog, schema, self._config.scopes)
        safe_table = sanitize_identifier(table, "table")
        safe_column = sanitize_identifier(metric_column, "column")
        
        metric_type_upper = metric_type.upper()
        valid_metrics = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
        if metric_type_upper not in valid_metrics:
            raise GuardrailError(
                f"Invalid metric_type '{metric_type}'. Must be one of: {', '.join(valid_metrics)}"
            )

        predicate_clause = f" WHERE {predicate}" if predicate else ""
        sql = f"SELECT {metric_type_upper}({safe_column}) as metric_value FROM `{catalog}`.`{schema}`.`{safe_table}`{predicate_clause}"
        timeout_value = effective_timeout(None, self._config.limits)
        rows, _ = self._execute(
            sql,
            params=None,
            limit=None,
            timeout=timeout_value,
            request_id=request_id,
        )
        metric_value = rows[0].get("metric_value") if rows else None
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "metric_type": metric_type_upper,
            "metric_column": metric_column,
            "metric_value": metric_value,
            "predicate": predicate,
        }

    def create_temp_table(
        self,
        temp_table_name: str,
        sql_query: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a session-scoped temporary view/table from a SQL query.

        This enables AI agents to create aggregate views combining multiple tables/views
        for lead generation analysis without exposing raw data. The temporary view exists
        only for the current session and is automatically cleaned up.

        The SQL query must be a SELECT statement and can include:
        - JOINs across multiple tables/views
        - Aggregations (COUNT, SUM, AVG, GROUP BY, etc.)
        - Filtering (WHERE clauses)
        - All referenced catalogs/schemas must be in the allowlist

        Parameters:
        temp_table_name (str): Name for the temporary view (must be valid identifier)
        sql_query (str): SELECT query to populate the temp view
        request_id (str | None): Request tracking ID

        Returns:
        dict[str, Any]: Metadata about created temp table including name and row count

        Raises:
        GuardrailError: If query violates guardrails or references disallowed catalogs/schemas
        QueryError: If query execution fails

        Example:
        create_temp_table(
            "leads_summary",
            '''
            SELECT 
                c.industry,
                COUNT(*) as lead_count,
                AVG(c.revenue) as avg_revenue
            FROM `main`.`default`.`companies` c
            JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
            WHERE c.revenue > 1000000
            GROUP BY c.industry
            '''
        )
        """
        # Validate temp table name
        safe_table_name = sanitize_identifier(temp_table_name, "temp_table_name")
        
        # Validate the SQL query against guardrails
        validate_temp_table_query(sql_query, self._config.scopes)
        
        # Create the temporary view using CREATE TEMPORARY VIEW
        create_sql = f"CREATE TEMPORARY VIEW {safe_table_name} AS {sql_query}"
        
        timeout_value = effective_timeout(None, self._config.limits)
        
        # Execute the CREATE TEMPORARY VIEW statement
        # Note: CREATE is allowed if configured in allow_statement_types
        access_token = self._token_provider.get_token()
        query_id = str(uuid.uuid4())

        with self._semaphore:
            try:
                with databricks.sql.connect(
                    server_hostname=self._config.warehouse.host,
                    http_path=self._config.warehouse.http_path,
                    access_token=access_token,
                    session_configuration={
                        "ansi_mode": "true",
                    },
                ) as connection:
                    with connection.cursor() as cursor:
                        # Create the temporary view
                        cursor.execute(create_sql)
                        
                        # Get row count from the temp view
                        count_sql = f"SELECT COUNT(*) as row_count FROM {safe_table_name}"
                        cursor.execute(count_sql)
                        count_result = cursor.fetchone()
                        row_count = count_result[0] if count_result else 0
                        
            except DatabricksError as exc:
                self._log.warning(
                    "Temporary table creation failed",
                    extra=log_extra(
                        request_id=request_id,
                        query_id=query_id,
                        temp_table_name=safe_table_name,
                        error_message=str(exc),
                    ),
                )
                raise QueryError(
                    f"Failed to create temporary table '{safe_table_name}': {exc}"
                ) from exc

        self._log.info(
            "Temporary table created",
            extra=log_extra(
                request_id=request_id,
                query_id=query_id,
                temp_table_name=safe_table_name,
                row_count=row_count,
            ),
        )
        
        return {
            "temp_table_name": safe_table_name,
            "row_count": row_count,
            "status": "created",
            "scope": "session",
            "note": "This temporary view is session-scoped and will be automatically cleaned up when the session ends",
        }

    def _execute(
        self,
        sql: str,
        params: Iterable[Any] | None,
        limit: int | None,
        timeout: int | None = None,
        request_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """
        Execute SQL query and return results as list of dictionaries.

        Assumes LIMIT is already applied in the SQL query for large datasets.
        Timeout is enforced at session level (30s default); per-query timeout not supported by Databricks.

        Parameters:
        sql (str): SQL query to execute
        params (Iterable[Any] | None): Query parameters for parameterized queries
        limit (int | None): Expected row limit (for logging and validation)
        timeout (int | None): Deprecated; Databricks enforces session-level timeout instead
        request_id (str | None): Request tracking ID

        Returns:
        tuple[list[dict[str, Any]], bool]: Rows as dictionaries and truncated flag

        Raises:
        QueryError: If query execution fails
        """
        statement_type = detect_statement_type(sql)
        ensure_statement_allowed(
            statement_type, self._config.limits.allow_statement_types
        )

        access_token = self._token_provider.get_token()
        query_id = str(uuid.uuid4())

        with self._semaphore:
            try:
                with databricks.sql.connect(
                    server_hostname=self._config.warehouse.host,
                    http_path=self._config.warehouse.http_path,
                    access_token=access_token,
                    session_configuration={
                        "ansi_mode": "true",
                    },
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql, params)
                        rows_raw = cursor.fetchall()
                        description = cursor.description or []
            except DatabricksError as exc:
                self._log.warning(
                    "Databricks query failed",
                    extra=log_extra(
                        request_id=request_id,
                        query_id=query_id,
                        statement_type=statement_type,
                        error_message=str(exc),
                    ),
                )
                raise QueryError(
                    f"Query execution failed: {exc}"
                ) from exc

        columns = [col[0] for col in description]
        rows = [dict(zip(columns, row)) for row in rows_raw]
        truncated = False

        self._log.info(
            "Query executed",
            extra=log_extra(
                request_id=request_id,
                query_id=query_id,
                statement_type=statement_type,
                truncated=truncated,
            ),
        )
        return rows, truncated
