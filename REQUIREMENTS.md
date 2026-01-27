---
post_title: Requirements - Databricks MCP Lead Generation Server
author1: GitHub Copilot
post_slug: requirements-databricks-mcp-server
microsoft_alias: na
featured_image: na
categories:
  - uncategorized
tags:
  - databricks
  - mcp
  - requirements
  - lead-generation
ai_note: Generated with AI assistance
summary: Requirements and constraints for the Databricks MCP lead generation server.
post_date: 2026-01-27
---

## Requirements: Databricks MCP Lead Generation Server

## Goal and Scope
- Expose Databricks data through an MCP server to enable lead generation and audience sizing via aggregated metrics only.
- Never return raw data; all results must be aggregated values (COUNT, SUM, AVG, MIN, MAX).
- Limit access to explicitly allowed catalogs and schemas; prevent access to anything outside that allowlist.
- Use a Databricks Serverless SQL Warehouse for execution to simplify scaling and isolate workloads.
- Runtime: Python FastMCP with a Databricks SQL client wrapper (runtime defined in project requirements, not config).
- Support exporting conditions as Jupyter notebook generation parameters for internal DS team analysis on Databricks platform.
- Out of scope: raw data retrieval, data writes/DDL, Unity Catalog management, job orchestration, lineage, or fine-grained row-level authorization beyond what Databricks provides.

## Personas
- Platform engineer: configures warehouse access, catalogs/schemas, and deployment environment.
- Lead generation analyst: uses MCP-compatible agents to explore tables, build conditions, and estimate audience size via aggregated metrics.
- Data scientist (internal): receives exported conditions to generate Jupyter notebooks and perform detailed analysis on Databricks platform.

## Authentication and Authorization
- OAuth service principal only (shared principal for this POC).
- All secrets provided via env vars or secret manager; never committed.
- Scope enforcement
  - Configurable allowlists for catalogs and schemas; default deny for everything else.
  - Validation for incoming tool calls so any request outside scope is rejected with a clear error.

## Tools (Minimum Set)

### Metadata Discovery (No Data Exposure)
- `list_catalogs`: return only allowed items.
- `list_schemas`: list schemas per catalog with pagination if needed.
- `list_tables`: list tables/views per schema with basic stats if available.
- `table_metadata`: columns, data types, nullability, comments, primary/unique keys, partition columns, and row counts when available.
- `partition_info`: explicit tool to surface partition columns and statistics to guide efficient queries.

### Audience Sizing & Aggregation (Data-Agnostic Only)
- `approx_count(catalog, schema, table, predicate?)`: Get approximate row count with optional filtering for audience size estimation. No raw data returned.
- `aggregate_metric(catalog, schema, table, metric_type, metric_column, predicate?)`: Calculate aggregated metrics (COUNT, SUM, AVG, MIN, MAX) without returning individual rows.
  - `metric_type`: One of COUNT, SUM, AVG, MIN, MAX
  - `metric_column`: Column to aggregate (use "*" for COUNT)
  - `predicate`: Optional WHERE clause for filtering before aggregation

## Guardrails
- Global max rows, max execution time, and max concurrent queries.
- Statement allowlist: SELECT-only for aggregation queries.
- Per-tool row/time limits always enforced.
- No data exposure: all results must be aggregated values only.
- Any raw data retrieval attempt is rejected with clear error.

## Error Handling
- Return concise, user-friendly errors without leaking raw SQL or stack traces.
- Map common Databricks errors (auth, permission denied, syntax, timeout) to stable error codes/messages.

## Observability
- Structured logs with request IDs, query IDs, Databricks statement IDs, and duration.
- Basic metrics: request counts by tool, error counts, p95 latency.
- Log aggregation operations but never log actual metric values in excessive detail to prevent data exposure.

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
  (default `["SELECT"]`). `-1` may be used to indicate no limit where explicitly allowed.
- Prefer YAML config file to define allowed catalogs and per-catalog schemas; env vars
  supply secrets.
- Configuration validation on startup with clear failure messages.

## Performance Considerations
- Favor Serverless SQL Warehouse; document recommended size and auto-stop policy.
- Use aggregation at database level; never fetch raw data then aggregate in application layer.
- Prefer metadata APIs or DESCRIBE statements that avoid full scans.

## Security and Compliance
- Enforce TLS for external exposure.
- Do not log sensitive data, raw SQL, or metric values in excessive detail.
- Respect Databricks access controls; if permissions deny access, surface a permission error.
- No lineage exposure in this POC.
- All aggregation results safe for external consumption (no PII or raw data exposure).

## Testing
- Unit tests for config parsing, scope enforcement, and guardrails.
- Integration tests against a test warehouse for metadata listing and aggregation queries.
- Tests verify that no raw data is ever returned, even in error scenarios.

## Deployment Expectations
- Service can run on a small container or serverless function with outbound access to Databricks workspace.
- Health endpoint for liveness; optional readiness that verifies warehouse connectivity with a lightweight ping.
- Provide example MCP server config and sample tool calls for agent integrators.

## Open Questions / Future Enhancements
- Condition export format for Jupyter notebook generation (JSON, YAML, or native Python)?
- Should aggregations support GROUP BY for multi-dimensional audience segmentation?
- Row-level filtering based on user attributes for additional governance?


