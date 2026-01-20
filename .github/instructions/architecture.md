---
applyTo: "**"
description: "Architecture notes for Databricks MCP server"
---

# Architecture Notes
- Components
  - Config loader: merge YAML config + env vars (secrets in env only), validate allowlists and limits (-1 means no cap where allowed).
  - Databricks SQL client wrapper: handles OAuth token retrieval, statement execution, pagination/truncation, and error normalization.
  - Tool handlers: `list_catalogs`, `list_schemas`, `list_tables`, `table_metadata`, `partition_info`, `sample_data`, `preview_query`, `run_query`, `health_check`.
  - Observability: structured logging with request/query IDs; metrics for counts, errors, latency, rows.
- Guardrails
  - Enforce allowlisted catalogs/schemas on every tool; reject anything else with clear errors.
  - SELECT-only default; require explicit config to allow DDL/DML (discouraged for this POC).
  - Apply max rows/time and sample caps server-side even if caller asks for more.
- Data handling
  - Use lightweight metadata APIs or DESCRIBE to avoid full scans.
  - Truncate large results and indicate truncation; never log full result sets or secrets.
- Operational
  - Health endpoint avoids Databricks calls; readiness may ping warehouse.
  - Assume Serverless SQL Warehouse connectivity with TLS.
