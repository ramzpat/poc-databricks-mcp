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

    def create_temp_view(
        self,
        temp_table_name: str,
        source_tables: list[dict[str, str]],
        columns: list[dict[str, str]],
        join_conditions: list[dict[str, str]] | None = None,
        where_conditions: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a temporary table by combining multiple views or data sources.

        This enables AI agents to aggregate data from multiple tables/views with structured 
        business logic for lead generation purposes. The temporary table is session-scoped
        and will be automatically deleted when the Databricks session ends.
        
        IMPORTANT: Temporary tables created with this tool are NOT persistent. They exist only 
        within the current Databricks session and will be automatically deleted when the session 
        terminates. They cannot be accessed from other AI agent sessions.

        Parameters:
        temp_table_name (str): Name for the temporary view (alphanumeric and underscores only)
        source_tables (list[dict]): List of source tables, each with:
            - catalog (str): Catalog name (must be allowlisted)
            - schema (str): Schema name (must be allowlisted)
            - table (str): Table or view name
            - alias (str): Alias to use in the query (e.g., "t1", "customers")
        columns (list[dict]): List of columns to include, each with:
            - table_alias (str): Alias of the source table
            - column (str): Column name from that table
            - alias (str, optional): Alias for the column in the result (if renaming)
        join_conditions (list[dict], optional): List of JOIN specifications, each with:
            - type (str): Join type - "INNER", "LEFT", "RIGHT", or "FULL" (default: "INNER")
            - left_table (str): Alias of the left table
            - left_column (str): Column from left table
            - right_table (str): Alias of the right table
            - right_column (str): Column from right table
        where_conditions (str, optional): WHERE clause conditions (without the WHERE keyword)
        request_id (str | None): Request tracking ID

        Returns:
        dict[str, Any]: Metadata about the created temporary table including row count

        Raises:
        GuardrailError: If parameters are invalid or tables not in allowlist
        QueryError: If table creation fails

        Example:
        source_tables = [
            {"catalog": "main", "schema": "sales", "table": "purchases", "alias": "p"},
            {"catalog": "main", "schema": "analytics", "table": "engagement", "alias": "e"}
        ]
        columns = [
            {"table_alias": "p", "column": "customer_id"},
            {"table_alias": "p", "column": "total_purchases", "alias": "total_spent"},
            {"table_alias": "e", "column": "engagement_score"}
        ]
        join_conditions = [
            {
                "type": "INNER",
                "left_table": "p",
                "left_column": "customer_id",
                "right_table": "e",
                "right_column": "customer_id"
            }
        ]
        where_conditions = "p.total_purchases > 1000 AND e.engagement_score > 0.7"
        """
        # Validate temp table name
        safe_temp_table = sanitize_identifier(temp_table_name, "temp_table_name")
        
        # Validate source tables and check allowlist
        if not source_tables or len(source_tables) == 0:
            raise GuardrailError("At least one source table is required")
        
        validated_tables = []
        for idx, table_spec in enumerate(source_tables):
            if not isinstance(table_spec, dict):
                raise GuardrailError(f"source_tables[{idx}] must be a dictionary")
            
            catalog = table_spec.get("catalog", "")
            schema = table_spec.get("schema", "")
            table = table_spec.get("table", "")
            alias = table_spec.get("alias", "")
            
            if not catalog or not schema or not table or not alias:
                raise GuardrailError(
                    f"source_tables[{idx}] must have 'catalog', 'schema', 'table', and 'alias'"
                )
            
            # Check allowlist
            ensure_catalog_allowed(catalog, self._config.scopes)
            ensure_schema_allowed(catalog, schema, self._config.scopes)
            
            # Sanitize identifiers
            safe_catalog = sanitize_identifier(catalog, "catalog")
            safe_schema = sanitize_identifier(schema, "schema")
            safe_table = sanitize_identifier(table, "table")
            safe_alias = sanitize_identifier(alias, "alias")
            
            validated_tables.append({
                "catalog": safe_catalog,
                "schema": safe_schema,
                "table": safe_table,
                "alias": safe_alias,
            })
        
        # Validate and build column list
        if not columns or len(columns) == 0:
            raise GuardrailError("At least one column is required")
        
        column_parts = []
        for idx, col_spec in enumerate(columns):
            if not isinstance(col_spec, dict):
                raise GuardrailError(f"columns[{idx}] must be a dictionary")
            
            table_alias = col_spec.get("table_alias", "")
            column = col_spec.get("column", "")
            col_alias = col_spec.get("alias")
            
            if not table_alias or not column:
                raise GuardrailError(
                    f"columns[{idx}] must have 'table_alias' and 'column'"
                )
            
            # Sanitize identifiers
            safe_table_alias = sanitize_identifier(table_alias, "table_alias")
            safe_column = sanitize_identifier(column, "column")
            
            if col_alias:
                safe_col_alias = sanitize_identifier(col_alias, "column_alias")
                column_parts.append(f"`{safe_table_alias}`.`{safe_column}` AS `{safe_col_alias}`")
            else:
                column_parts.append(f"`{safe_table_alias}`.`{safe_column}`")
        
        # Build FROM clause with first table
        first_table = validated_tables[0]
        from_clause = (
            f"FROM `{first_table['catalog']}`.`{first_table['schema']}`.`{first_table['table']}` AS `{first_table['alias']}`"
        )
        
        # Build JOIN clauses
        join_clause = ""
        if join_conditions and len(join_conditions) > 0:
            join_parts = []
            for idx, join_spec in enumerate(join_conditions):
                if not isinstance(join_spec, dict):
                    raise GuardrailError(f"join_conditions[{idx}] must be a dictionary")
                
                join_type = join_spec.get("type", "INNER").upper()
                left_table = join_spec.get("left_table", "")
                left_column = join_spec.get("left_column", "")
                right_table = join_spec.get("right_table", "")
                right_column = join_spec.get("right_column", "")
                
                if not left_table or not left_column or not right_table or not right_column:
                    raise GuardrailError(
                        f"join_conditions[{idx}] must have 'left_table', 'left_column', 'right_table', and 'right_column'"
                    )
                
                if join_type not in {"INNER", "LEFT", "RIGHT", "FULL"}:
                    raise GuardrailError(
                        f"join_conditions[{idx}] type must be INNER, LEFT, RIGHT, or FULL"
                    )
                
                # Sanitize identifiers
                safe_left_table = sanitize_identifier(left_table, "left_table")
                safe_left_column = sanitize_identifier(left_column, "left_column")
                safe_right_table = sanitize_identifier(right_table, "right_table")
                safe_right_column = sanitize_identifier(right_column, "right_column")
                
                # Find the right table in validated_tables to get its full name
                right_table_spec = None
                for t in validated_tables:
                    if t["alias"] == safe_right_table:
                        right_table_spec = t
                        break
                
                if not right_table_spec:
                    raise GuardrailError(
                        f"join_conditions[{idx}] references unknown table alias '{right_table}'"
                    )
                
                join_parts.append(
                    f"{join_type} JOIN `{right_table_spec['catalog']}`.`{right_table_spec['schema']}`.`{right_table_spec['table']}` AS `{safe_right_table}` "
                    f"ON `{safe_left_table}`.`{safe_left_column}` = `{safe_right_table}`.`{safe_right_column}`"
                )
            
            join_clause = " ".join(join_parts)
        
        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            # Simple validation - no semicolons allowed
            if ";" in where_conditions:
                raise GuardrailError("where_conditions cannot contain semicolons")
            where_clause = f"WHERE {where_conditions}"
        
        # Build complete SELECT query
        select_clause = f"SELECT {', '.join(column_parts)}"
        source_query = f"{select_clause} {from_clause} {join_clause} {where_clause}".strip()
        
        # Create temporary table using CREATE TEMPORARY TABLE
        # Note: Databricks doesn't support CREATE OR REPLACE for temp tables
        # So we drop first if exists, then create
        drop_sql = f"DROP TEMPORARY TABLE IF EXISTS `{safe_temp_table}`"
        create_sql = f"CREATE TEMPORARY TABLE `{safe_temp_table}` AS {source_query}"
        
        timeout_value = effective_timeout(None, self._config.limits)
        
        # Execute the create statement and count query in a single connection
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
                        # Drop the temporary table if it exists
                        cursor.execute(drop_sql, None)
                        
                        # Create the temporary table
                        cursor.execute(create_sql, None)
                        
                        # Get row count of the created temporary table
                        count_sql = f"SELECT COUNT(*) as row_count FROM `{safe_temp_table}`"
                        cursor.execute(count_sql, None)
                        count_rows = cursor.fetchall()
                        
            except DatabricksError as exc:
                self._log.warning(
                    "Temporary table creation failed",
                    extra=log_extra(
                        request_id=request_id,
                        temp_table_name=safe_temp_table,
                        error_message=str(exc),
                    ),
                )
                raise QueryError(
                    f"Failed to create temporary table '{safe_temp_table}': {exc}"
                ) from exc
        
        row_count = count_rows[0][0] if count_rows else 0
        
        self._log.info(
            "Temporary table created",
            extra=log_extra(
                request_id=request_id,
                temp_table_name=safe_temp_table,
                row_count=row_count,
            ),
        )
        
        return {
            "temp_table_name": safe_temp_table,
            "row_count": row_count,
            "status": "created",
            "note": "This temporary table is session-scoped and will be automatically deleted when the Databricks session ends. It cannot be accessed from other sessions.",
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
        # Allow CREATE TEMPORARY TABLE and DROP TEMPORARY TABLE for temporary table management
        sql_upper = sql.strip().upper()
        is_temp_table_op = (
            (statement_type == "CREATE" and sql_upper.startswith("CREATE TEMPORARY TABLE"))
            or (statement_type == "DROP" and sql_upper.startswith("DROP TEMPORARY TABLE"))
        )
        if not is_temp_table_op:
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
