---
applyTo: "**/*.md"
description: "Documentation standards for Databricks MCP"
---

# Documentation Standards
- Keep README updated when tool surface, guardrails, or config options change (per update-docs-on-code-change policy).
- Include minimal runnable examples: sample YAML config, example tool calls, and guardrail defaults (row/time caps, SELECT-only).
- Document required env vars for auth (client ID/secret, host, http path, warehouse ID); never include real secrets.
- Describe how to interpret truncated results and error messages; note that raw SQL/stack traces are intentionally hidden.
- Prefer concise bullet lists and tables over prose; keep sections short and action-oriented.
