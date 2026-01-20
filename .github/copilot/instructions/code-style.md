---
applyTo: "**/*.py"
description: "Python style for Databricks MCP server"
---

# Python Code Style (Databricks MCP)
- Target Python 3.11+; prefer standard lib + dataclasses/typing; avoid unused dependencies.
- Use type hints everywhere; enable `from __future__ import annotations` if needed for forward refs.
- Favor small, pure helpers; keep functions <30 lines when possible.
- Logging: structured key/value (request_id, query_id, statement_id); no raw SQL or secrets in logs.
- Errors: raise typed exceptions mapped to MCP tool errors; messages concise and user-safe.
- Config: load from YAML + env, validate eagerly; forbid missing required fields and non-SELECT statements unless explicitly allowed.
- SQL execution: parameterize inputs; enforce row/time limits server-side; block DDL/DML by default.
- Concurrency: guard max concurrent queries with a simple semaphore if using async; release on error paths.
- Tests: pytest with explicit asserts; cover scope enforcement and guardrails.
