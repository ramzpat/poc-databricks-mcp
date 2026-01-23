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
from .jobs import DatabricksJobsClient
from .logging_utils import configure_logging


def _request_id(value: str | None = None) -> str:
    return value or str(uuid.uuid4())


def _config_path() -> Path:
    path = os.environ.get("DATABRICKS_MCP_CONFIG", "config.example.yml")
    return Path(path)


def build_app(
    config: AppConfig,
    sql_client: DatabricksSQLClient,
    jobs_client: DatabricksJobsClient,
) -> FastMCP:
    app = FastMCP("databricks-mcp")

    @app.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_catalogs, rid)

    @app.tool()
    async def list_schemas(catalog: str, request_id: str | None = None) -> list[str]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_schemas, catalog, rid)

    @app.tool()
    async def list_tables(
        catalog: str, schema: str, request_id: str | None = None
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_tables, catalog, schema, rid)

    @app.tool()
    async def table_metadata(
        catalog: str,
        schema: str,
        table: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
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
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.partition_info, catalog, schema, table, rid
        )

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
        return await asyncio.to_thread(
            sql_client.sample_data, catalog, schema, table, limit, predicate, rid
        )

    @app.tool()
    async def preview_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.preview_query, sql, limit, timeout_seconds, rid
        )

    @app.tool()
    async def run_query(
        sql: str,
        limit: int | None = None,
        timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            sql_client.run_query, sql, limit, timeout_seconds, rid
        )

    @app.tool()
    async def execute_python_code(
        code: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute Python code via Databricks serverless compute."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(jobs_client.execute_python_code, code, rid)

    @app.tool()
    async def submit_python_job(
        job_name: str,
        code: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit a Python job for async execution."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            jobs_client.submit_python_job, job_name, code, rid
        )

    @app.tool()
    async def submit_notebook_job(
        job_name: str,
        notebook_path: str,
        parameters: dict[str, str] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit a notebook for async execution."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(
            jobs_client.submit_notebook_job, job_name, notebook_path, parameters, rid
        )

    @app.tool()
    async def get_job_status(
        run_id: int,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Get the status of a job run."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(jobs_client.get_job_status, run_id, rid)

    @app.tool()
    async def get_job_output(
        run_id: int,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve output and logs from a completed job run."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(jobs_client.get_job_run_output, run_id, rid)

    @app.tool()
    async def cancel_run(
        run_id: int,
        request_id: str | None = None,
    ) -> dict[str, str]:
        """Cancel an active job run."""
        rid = _request_id(request_id)
        await asyncio.to_thread(jobs_client.cancel_run, run_id, rid)
        return {"status": "cancelled", "run_id": str(run_id)}

    @app.tool()
    async def health_check() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:
    config = load_config(_config_path())
    configure_logging(config.observability.log_level)
    token_provider = OAuthTokenProvider(config.oauth)
    sql_client = DatabricksSQLClient(config, token_provider)
    jobs_client = DatabricksJobsClient(config, token_provider)
    app = build_app(config, sql_client, jobs_client)
    app.run()


if __name__ == "__main__":
    main()
