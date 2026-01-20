---
agent: "agent"
description: "Workflow prompt for adding/updating Databricks MCP tools"
model: "GPT-5.1-Codex-Max"
---

# Workflow: Add or Update an MCP Tool
1) Read [README.md](../../../README.md) and [REQUIREMENTS.md](../../../REQUIREMENTS.md) to confirm tool behavior, limits, and allowlists.
2) Identify required inputs/outputs; plan guardrails (SELECT-only default, row/time caps, pagination/truncation notes).
3) Touch only the targeted tool handler and shared client/config modules; avoid unrelated refactors.
4) Enforce scope: allowed catalogs/schemas; reject others early with clear errors.
5) Enforce limits server-side even if caller requests more; mark responses as truncated when applicable.
6) Map Databricks errors to user-safe messages; never include raw SQL or secrets.
7) Add or update tests (see testing-strategy) for config validation, scope enforcement, and limits.
8) Update docs/examples if behavior or config changes.

## Quick command examples (adapt to project structure)
- "Implement `partition_info` tool with partition columns/stats and allowlist checks."
- "Add row/time limit enforcement to `preview_query` and update tests."
- "Document new config flag to allow DDL (discouraged) and warn about risks."
