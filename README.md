## POC - Databricks MCP

### Overview
- Build a Model Context Protocol (MCP) server that fronts Databricks data so Agentic AIs can discover metadata, sample data, and run governed queries.
- The server should scope access to specific catalogs and schemas and rely on a Databricks Serverless SQL warehouse for execution.

### Intended Audience
- Platform engineers setting up Databricks access for AI agents.
- Data scientists and analysts using MCP-compatible agents to explore data.

### Proposed MCP Tools
- `list_catalogs` and `list_schemas`: enumerate allowed catalogs and schemas.
- `list_tables`: list tables/views within the allowed schemas.
- `table_metadata`: column types, nullable flags, comments, primary keys, partition columns, and row counts when available.
- `partition_info`: dedicated tool to surface partition columns and statistics to guide efficient queries.
- `sample_data`: lightweight sample with configurable row limit and optional predicate; must enforce row limits server-side.
- `preview_query`: run limited SELECT with timeout and max rows for quick validation.
- `run_query`: governed query execution with row limit, timeout, and optional pagination/cursor.
- `health_check`: simple readiness/liveness endpoint for hosting environment.

### Configuration
- Auth: OAuth service principal only (shared principal); tokens sourced from env/secret manager.
- Databricks connection via Serverless SQL Warehouse: host, HTTP path, warehouse ID.
- Allowed scopes: list of catalogs and schemas (exact match) provided in a YAML config file; deny all others.
- Query guardrails: max rows, max execution time, max concurrent queries, and statement type allowlist (SELECT only by default). Use `-1` to represent no limit where allowed.
- Sampling guardrails: fixed row cap (e.g., 100–1,000; `-1` means no cap) and optional random sampling strategy.
- Observability: request IDs, query IDs, and Databricks job/run IDs propagated in logs.
- Secrets: all tokens and client secrets must be injected via environment variables or secret manager, never hard-coded.

### Operational Assumptions
- Uses Databricks Serverless SQL Warehouse for isolation and autoscaling; warehouse ID must be provided.
- Outbound MCP server reachable externally with TLS; network egress must reach Databricks workspace URL.
- Error handling returns concise messages and avoids leaking raw SQL or stack traces.

### Development Plan (high level)
- Implement Python FastMCP service with Databricks SQL client wrapper.
- Implement auth/config loader (env vars + YAML → structured config) with validation for required fields.
- Add per-tool handlers with guardrails (limits, allowed catalogs/schemas).
- Add minimal tests for config parsing, catalog/schema filtering, sampling limits, and query guardrails.
- Provide example MCP server config and example agent prompt for use.

### Documentation
- See requirements in REQUIREMENTS.md for detailed functional and non-functional expectations.
