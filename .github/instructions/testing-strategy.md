---
applyTo: "**"
description: "Testing strategy for Databricks MCP server"
---

# Testing Strategy
- Framework: pytest; keep tests deterministic and isolated from prod resources.
- Unit tests
  - Config parsing/validation: required fields, -1 handling, statement allowlist, limits.
  - Scope enforcement: reject non-allowlisted catalogs/schemas for every tool.
  - Guardrails: enforce row/time caps and SELECT-only defaults.
  - Error mapping: Databricks errors → stable user messages without raw SQL.
- Integration tests (with test warehouse creds)
  - Metadata listing for allowed schemas only; ensure denied access is blocked.
  - `sample_data`, `preview_query`, `run_query` respect limits and indicate truncation.
- Observability checks: structured log fields present (request_id/query_id) where relevant.
- CI: fail fast on missing tests for new tools or config paths; keep runtime short.
