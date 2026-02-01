from __future__ import annotations

import logging
import uuid
from threading import Semaphore
from typing import Any, Iterable

import databricks.sql
from databricks.sql.exc import Error as DatabricksError

from ..auth import OAuthTokenProvider
from ..config import AppConfig
from ..errors import GuardrailError, QueryError
from ..guardrails import (
    clamp_limit,
    detect_statement_type,
    effective_timeout,
    ensure_catalog_allowed,
    ensure_schema_allowed,
    ensure_statement_allowed,
    sanitize_identifier,
)
from ..logging_utils import log_extra


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

    def preview_query(
        self,
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        ensure_statement_allowed(
            detect_statement_type(sql), self._config.limits.allow_statement_types
        )
        cap = (
            self._config.limits.sample_max_rows
            if self._config.limits.sample_max_rows != -1
            else None
        )
        effective_limit = clamp_limit(
            limit, cap if cap is not None else self._config.limits.max_rows
        )
        timeout_value = effective_timeout(timeout_seconds, self._config.limits)
        wrapped_sql = self._wrap_with_limit(sql, effective_limit)
        rows, truncated = self._execute(
            wrapped_sql,
            params=None,
            limit=effective_limit,
            timeout=timeout_value,
            request_id=request_id,
        )
        return {"rows": rows, "truncated": truncated, "limit_applied": effective_limit}

    def run_query(
        self,
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        ensure_statement_allowed(
            detect_statement_type(sql), self._config.limits.allow_statement_types
        )
        effective_limit = clamp_limit(limit, self._config.limits.max_rows)
        timeout_value = effective_timeout(timeout_seconds, self._config.limits)
        wrapped_sql = self._wrap_with_limit(sql, effective_limit)
        rows, truncated = self._execute(
            wrapped_sql,
            params=None,
            limit=effective_limit,
            timeout=timeout_value,
            request_id=request_id,
        )
        return {"rows": rows, "truncated": truncated, "limit_applied": effective_limit}

    def _wrap_with_limit(self, sql: str, limit: int | None) -> str:
        if limit is None or limit == -1:
            return sql
        return f"SELECT * FROM ({sql}) AS subquery LIMIT {limit}"

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

        Parameters:
        sql (str): SQL query to execute
        params (Iterable[Any] | None): Query parameters for parameterized queries
        limit (int | None): Expected row limit (for logging and validation)
        timeout (int | None): Query timeout in seconds
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
                    session_configuration={"ansi_mode": "true"},
                ) as connection:
                    with connection.cursor() as cursor:
                        try:
                            cursor.execute(sql, params, timeout=timeout)
                        except TypeError as exc:
                            if "timeout" not in str(exc):
                                raise
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
