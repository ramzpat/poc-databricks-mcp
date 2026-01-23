---
post_title: POC - Databricks MCP
author1: GitHub Copilot
post_slug: poc-databricks-mcp
microsoft_alias: na
featured_image: na
categories:
	- uncategorized
tags:
	- databricks
	- mcp
	- configuration
ai_note: Generated with AI assistance
summary: Overview and configuration guide for the Databricks MCP server.
post_date: 2026-01-23
---

## POC - Databricks MCP

## Quick start
- Install: `pip install -e .`
- Copy `config.example.yml` to `config.yml` and fill in warehouse + allowlists; inject secrets via env (no secrets in files).
- Run: `DATABRICKS_MCP_CONFIG=config.yml python -m databricks_mcp.server`
- Tools expose metadata discovery, sampling, and governed query execution over an allowlisted catalog/schema set.

## Quick setup (dev)
- Create venv and install: `python -m venv .venv && source .venv/bin/activate && pip install -e .[test]`
- Export required env vars (example):
	- `export DATABRICKS_HOST="https://<workspace-host>"`
	- `export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<http-path>"`
	- `export DATABRICKS_WAREHOUSE_ID="<warehouse-id>"`
	- `export DATABRICKS_CLIENT_ID="<client-id>"`
	- `export DATABRICKS_CLIENT_SECRET="<client-secret>"`
	- `export DATABRICKS_TOKEN_URL="https://<workspace-host>/oidc/v1/token"`
- Copy `config.example.yml` to `config.yml` and adjust allowlists/limits; leave secrets as `${...}` env references.
- Run server: `DATABRICKS_MCP_CONFIG=config.yml python -m databricks_mcp.server`
- Run tests: `pytest`

## Get an OAuth token (service principal)
- Prereq: service principal with `client_id` / `client_secret`, and token endpoint (workspace OIDC). Scope is usually not required; if your IdP enforces it, pass `scope`.
- Example request:
	```bash
	curl -X POST "$DATABRICKS_TOKEN_URL" \
		-H 'Content-Type: application/x-www-form-urlencoded' \
		-d "grant_type=client_credentials" \
		-d "client_id=$DATABRICKS_CLIENT_ID" \
		-d "client_secret=$DATABRICKS_CLIENT_SECRET"
	```
- Expected JSON: `{ "access_token": "...", "token_type": "Bearer", "expires_in": 3600 }`
- The server fetches and refreshes tokens automatically via `DATABRICKS_CLIENT_ID/SECRET` and `DATABRICKS_TOKEN_URL`; the curl is only for validation/debug.

## Configuration
- Required fields (YAML): warehouse host, `http_path`, `warehouse_id`,
	allowlisted `scopes.catalogs.<catalog>.schemas`, limits (rows/time/concurrency),
	and optional observability settings.
- Required env vars: OAuth client secret (`${DATABRICKS_CLIENT_SECRET}` in YAML)
	plus any host/token settings referenced with `${...}` (e.g.,
	`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_WAREHOUSE_ID`,
	`DATABRICKS_TOKEN_URL`).
- Limits: `max_rows`, `sample_max_rows`, `query_timeout_seconds`,
	`max_concurrent_queries` (cannot be unlimited), `allow_statement_types`
	(default `SELECT`). `-1` means “no limit” for rows/time only.
- Secrets: always use `${ENV_VAR}` references in YAML; never hard-code tokens.

## Tools
- `list_catalogs`, `list_schemas(catalog)`: return only allowlisted scopes.
- `list_tables(catalog, schema)`: tables/views from information schema within allowed scopes.
- `table_metadata(catalog, schema, table)`: columns, primary keys, partition columns, and row counts when available.
- `partition_info(catalog, schema, table)`: partition columns plus lightweight stats (row count, size).
- `sample_data(catalog, schema, table, limit?, predicate?)`: capped sample enforced server-side.
- `preview_query(sql, limit?, timeout_seconds?)`: SELECT-only quick check with strict row/time cap.
- `run_query(sql, limit?, timeout_seconds?)`: governed SELECT with row/time caps and optional unlimited rows when config explicitly uses `-1`.
- `health_check()`: liveness without contacting Databricks.

## Guardrails
- Allowlist enforcement for catalogs and schemas on every tool; anything else is rejected.
- SELECT-only by default; other statement types require explicit `allow_statement_types` in config.
- Server-side row caps, timeouts, and concurrency limits always applied (client requests cannot override).
- Truncation is indicated in results when limits cut output.

## Observability
- Structured logs include request IDs/query IDs; configure log level via `observability.log_level`.
- Errors are concise and avoid raw SQL or secrets; Databricks failures map to user-safe messages.

## Testing
- Install dev deps: `pip install -e .[test]`
- Run unit tests: `pytest`
