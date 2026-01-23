---
post_title: Requirements - Databricks MCP Server
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
ai_note: Generated with AI assistance
summary: Requirements and constraints for the Databricks MCP server.
post_date: 2026-01-23
---

## Requirements: Databricks MCP Server

## Goal and Scope
- Expose Databricks data through an MCP server so external Agentic AIs can explore metadata, preview data, and run governed SQL queries.
- Limit access to explicitly allowed catalogs and schemas; prevent access to anything outside that allowlist.
- Use a Databricks Serverless SQL Warehouse for execution to simplify scaling and isolate workloads.
- Runtime: Python FastMCP with a Databricks SQL client wrapper (runtime defined in project requirements, not config).
- Out of scope: data writes/DDL, Unity Catalog management, job orchestration, lineage, or fine-grained row-level authorization beyond what Databricks provides.

## Personas
- Platform engineer: configures warehouse access, catalogs/schemas, and deployment environment.
- Data scientist/analyst: uses MCP-compatible agents to explore tables and run governed queries.

- Authentication to Databricks
  - OAuth service principal only (shared principal for this POC).
  - All secrets provided via env vars or secret manager; never committed.
- Scope enforcement
  - Configurable allowlists for catalogs and schemas; default deny for everything else.
  - Validation for incoming tool calls so any request outside scope is rejected with a clear error.
- Tools (minimum set)
  - `list_catalogs`, `list_schemas`: return only allowed items with pagination if needed.
  - `list_tables`: list tables/views per schema with basic stats if available.
  - `table_metadata`: columns, data types, nullability, comments, primary/unique keys, partition columns, and row counts when available.
  - `partition_info`: explicit tool to surface partition columns and statistics to guide efficient queries.
  - `sample_data`: capped row sample (configurable cap, default <= 1,000; `-1` means no cap but still server-enforced). Optional predicate supported.
  - `preview_query`: run SELECT with strict limits (row cap, timeout) for quick validation; `-1` indicates no cap where allowed.
  - `run_query`: governed SELECT with row limit, timeout, and optional pagination/cursor; block non-SELECT unless explicitly enabled. `-1` can represent no row cap if explicitly configured.
  - `health_check`: readiness/liveness response without hitting Databricks.
- Guardrails
  - Global max rows, max execution time, and max concurrent queries.
  - Statement allowlist: SELECT-only by default; explicit config required to allow DDL/DML (discouraged in this POC).
  - Per-tool row/time limits; sampling always enforced even if client asks for more.
- Error handling
  - Return concise, user-friendly errors without leaking raw SQL or stack traces.
  - Map common Databricks errors (auth, permission denied, syntax, timeout) to stable error codes/messages.
- Observability
  - Structured logs with request IDs, query IDs, Databricks statement IDs, and duration.
  - Basic metrics: request counts by tool, error counts, p95 latency, rows returned.
- Configuration
  - Required: DATABRICKS_HOST (workspace URL), DATABRICKS_HTTP_PATH (SQL Warehouse path),
    OAuth client credentials, DATABRICKS_WAREHOUSE_ID, allowlisted catalogs with
    per-catalog schemas defined in YAML config.
  - Optional: MAX_ROWS, SAMPLE_MAX_ROWS, QUERY_TIMEOUT_SECONDS, MAX_CONCURRENT_QUERIES,
    ALLOW_STATEMENT_TYPES (default ["SELECT"]). `-1` may be used to indicate no limit where
    explicitly allowed.
  - Prefer YAML config file to define allowed catalogs and per-catalog schemas; env vars
    supply secrets.
  - Configuration validation on startup with clear failure messages.
- Performance considerations
  - Favor Serverless SQL Warehouse; document recommended size and auto-stop policy.
  - Use result-set truncation and pagination for large outputs.
  - Prefer metadata APIs or DESCRIBE statements that avoid full scans.
- Security and compliance
  - Enforce TLS for external exposure.
  - Do not log sensitive data or full result sets; truncate or hash if necessary.
  - Respect Databricks access controls; if Unity Catalog permissions deny access, surface a permission error.
  - No lineage exposure in this POC.
- Testing
  - Unit tests for config parsing, scope enforcement, and guardrails.
  - Integration tests against a test warehouse for metadata listing, sampling, and limited queries.

## Deployment Expectations
- Service can run on a small container or serverless function with outbound access to Databricks workspace.
- Health endpoint for liveness; optional readiness that verifies warehouse connectivity with a lightweight ping.
- Provide example MCP server config and sample tool calls for agent integrators.

## Open Questions
- Any data masking or row-level filters required for specific schemas?
