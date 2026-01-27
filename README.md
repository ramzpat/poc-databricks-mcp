---
post_title: POC - Databricks MCP Lead Generation
author1: GitHub Copilot
post_slug: poc-databricks-mcp
microsoft_alias: na
featured_image: na
categories:
	- uncategorized
tags:
	- databricks
	- mcp
	- lead-generation
	- audience-sizing
ai_note: Generated with AI assistance
summary: Lead generation MCP server for audience sizing with aggregated metrics from Databricks.
post_date: 2026-01-27
---

## POC - Databricks MCP (Lead Generation Edition)

An MCP server that enables lead generation and audience sizing by providing approximate counts and aggregated metrics from Databricks, while preserving metadata checking capabilities for data governance.

### Key Features
- **Privacy-First Design**: No raw data is exposed; only aggregated metrics (COUNT, SUM, AVG, MIN, MAX)
- **Lead Audience Sizing**: Estimate audience size based on business logic conditions
- **Metadata Discovery**: Full access to table structure, columns, and partition information
- **Governed Access**: Allowlist-based catalog/schema access with guardrails
- **Condition-to-Notebook Pipeline**: Export conditions to generate Jupyter notebooks for internal DS team

## Quick start
- Install: `pip install -e .`
- Copy `config.example.yml` to `config.yml` and fill in warehouse + allowlists; inject secrets via env (no secrets in files).
- Run: `DATABRICKS_MCP_CONFIG=config.yml python -m databricks_mcp.server`
- Tools expose metadata discovery and privacy-safe aggregated metrics over an allowlisted catalog/schema set.

## Remote Usage with Claude Desktop or Other MCP Clients

This MCP server can be used remotely without cloning the repository. Use `uvx` (part of `uv` package manager) to run the server directly from the Git repository.

### Prerequisites
1. Install `uv` (includes `uvx`): https://docs.astral.sh/uv/getting-started/installation/
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Create a configuration file `config.yml` in a known location on your system
3. Set up your Databricks credentials as environment variables

### Using with Claude Desktop (Recommended)

#### Step 1: Create your config file
Create a `config.yml` file anywhere on your system (e.g., `~/.config/databricks-mcp/config.yml` on macOS/Linux or `%USERPROFILE%\.config\databricks-mcp\config.yml` on Windows):

```yaml
warehouse:
  host: ${DATABRICKS_HOST}
  http_path: ${DATABRICKS_HTTP_PATH}
  warehouse_id: ${DATABRICKS_WAREHOUSE_ID}
auth:
  oauth:
    client_id: ${DATABRICKS_CLIENT_ID}
    client_secret: ${DATABRICKS_CLIENT_SECRET}
    token_url: ${DATABRICKS_TOKEN_URL}
scopes:
  catalogs:
    main:
      schemas:
        - default
    # Add more catalogs as needed
    # my_catalog:
    #   schemas:
    #     - schema1
    #     - schema2
limits:
  max_rows: 10000
  sample_max_rows: 1000
  query_timeout_seconds: 60
  max_concurrent_queries: 5
  allow_statement_types:
    - SELECT
observability:
  log_level: info
  propagate_request_ids: true
```

#### Step 2: Configure Claude Desktop

Edit your Claude Desktop configuration file:
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

Add this configuration to run from the Git repository:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "command": "uvx",
      "args": [
        "--python",
        "3.11",
        "--from",
        "git+https://github.com/ramzpat/poc-databricks-mcp.git",
        "databricks-mcp"
      ],
      "env": {
        "DATABRICKS_MCP_CONFIG": "/absolute/path/to/your/config.yml",
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_WAREHOUSE_ID": "your-warehouse-id",
        "DATABRICKS_CLIENT_ID": "your-client-id",
        "DATABRICKS_CLIENT_SECRET": "your-client-secret",
        "DATABRICKS_TOKEN_URL": "https://your-workspace.cloud.databricks.com/oidc/v1/token"
      }
    }
  }
}
```

**Important Notes:**
- Replace `/absolute/path/to/your/config.yml` with the actual absolute path to your config file
- Replace all `your-*` placeholders with your actual Databricks credentials
- On Windows, use forward slashes or double backslashes in paths: `C:/Users/YourName/.config/databricks-mcp/config.yml` or `C:\\Users\\YourName\\.config\\databricks-mcp\\config.yml`

#### Step 3: Restart Claude Desktop

After saving the configuration, restart Claude Desktop. The Databricks MCP server will be available automatically.

### Alternative: Using from PyPI (When Published)

If this package is published to PyPI, you can simplify the configuration:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "command": "uvx",
      "args": [
        "--python",
        "3.11",
        "databricks-mcp"
      ],
      "env": {
        "DATABRICKS_MCP_CONFIG": "/absolute/path/to/your/config.yml",
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_WAREHOUSE_ID": "your-warehouse-id",
        "DATABRICKS_CLIENT_ID": "your-client-id",
        "DATABRICKS_CLIENT_SECRET": "your-client-secret",
        "DATABRICKS_TOKEN_URL": "https://your-workspace.cloud.databricks.com/oidc/v1/token"
      }
    }
  }
}
```

### Alternative: Config File in Current Working Directory

If you prefer to place your `config.yml` in a specific directory, you can set that as the working directory or use an absolute path:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "command": "uvx",
      "args": [
        "--python",
        "3.11",
        "--from",
        "git+https://github.com/ramzpat/poc-databricks-mcp.git",
        "databricks-mcp"
      ],
      "env": {
        "DATABRICKS_MCP_CONFIG": "config.yml",
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_WAREHOUSE_ID": "your-warehouse-id",
        "DATABRICKS_CLIENT_ID": "your-client-id",
        "DATABRICKS_CLIENT_SECRET": "your-client-secret",
        "DATABRICKS_TOKEN_URL": "https://your-workspace.cloud.databricks.com/oidc/v1/token"
      }
    }
  }
}
```

**Note**: If `DATABRICKS_MCP_CONFIG` is not set, the server will look for `config.example.yml` in the current working directory by default. For production use, always specify an absolute path to your `config.yml` file for clarity and reliability.

### Environment Variables Reference

All configuration values support environment variable substitution using `${VAR_NAME}` syntax in the `config.yml` file. You can choose to:

1. **Store secrets only in environment variables** (recommended):
   - Keep sensitive values (client_secret, tokens) only in the MCP client config's `env` section
   - Reference them in `config.yml` with `${VARIABLE_NAME}`

2. **Mix config file and environment variables**:
   - Store non-sensitive config in `config.yml`
   - Store secrets as environment variables

Example with minimal environment variables:
```json
{
  "mcpServers": {
    "databricks-mcp": {
      "command": "uvx",
      "args": [
        "--python",
        "3.11",
        "--from",
        "git+https://github.com/ramzpat/poc-databricks-mcp.git",
        "databricks-mcp"
      ],
      "env": {
        "DATABRICKS_MCP_CONFIG": "/path/to/config.yml",
        "DATABRICKS_CLIENT_SECRET": "your-secret-here"
      }
    }
  }
}
```

And in `config.yml`, hardcode non-sensitive values:
```yaml
warehouse:
  host: "https://your-workspace.cloud.databricks.com"
  http_path: "/sql/1.0/warehouses/warehouse-id"
  warehouse_id: "warehouse-id"
auth:
  oauth:
    client_id: "your-client-id"
    client_secret: ${DATABRICKS_CLIENT_SECRET}  # From environment
    token_url: "https://your-workspace.cloud.databricks.com/oidc/v1/token"
```

### Testing Your Configuration

To verify your configuration works, you can test it manually from the command line:

```bash
# Set environment variables
export DATABRICKS_MCP_CONFIG="/path/to/your/config.yml"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"
export DATABRICKS_TOKEN_URL="https://your-workspace.cloud.databricks.com/oidc/v1/token"

# Run with uvx from Git (if uv is installed)
uvx --python 3.11 --from git+https://github.com/ramzpat/poc-databricks-mcp.git databricks-mcp

# Or run directly with Python (if you cloned the repo)
python -m databricks_mcp.server
```

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

### Metadata Discovery (No Data Exposure)
- **`list_catalogs`**: Return only allowlisted catalogs.
- **`list_schemas(catalog)`**: List all schemas within an allowed catalog.
- **`list_tables(catalog, schema)`**: Tables/views from information schema within allowed scopes.
- **`table_metadata(catalog, schema, table)`**: Columns, data types, primary keys, partition columns, and approximate row counts.
- **`partition_info(catalog, schema, table)`**: Partition columns plus lightweight statistics (row count, size).

### Lead Generation & Audience Sizing (Aggregated Only)
- **`approx_count(catalog, schema, table, predicate?)`**: Approximate row count for audience sizing with optional WHERE clause filtering. No raw data returned.
- **`aggregate_metric(catalog, schema, table, metric_type, metric_column, predicate?)`**: Calculate aggregated metrics (COUNT, SUM, AVG, MIN, MAX) without returning individual rows.
  - `metric_type`: One of COUNT, SUM, AVG, MIN, MAX
  - `metric_column`: Column to aggregate on (use "*" for COUNT)
  - `predicate`: Optional WHERE clause to filter rows before aggregation

## Guardrails
- Allowlist enforcement for catalogs and schemas on every tool; anything else is rejected.
- SELECT-only statements (enforced for aggregation queries).
- Server-side row caps and timeouts always applied.
- No raw data retrieval; all results are aggregated metrics only.

## Use Case: Lead Generation Workflow

1. **Explore**: Use metadata tools to understand table structure and available columns.
2. **Define Conditions**: Work with the MCP server to build WHERE clause predicates based on business logic.
3. **Size Audience**: Use `approx_count` and `aggregate_metric` to estimate audience size with various conditions.
4. **Export & Analyze**: Export conditions to generate a Jupyter notebook for internal DS team to run on Databricks platform for detailed analysis.

## Observability
- Structured logs include request IDs/query IDs; configure log level via `observability.log_level`.
- Errors are concise and avoid raw SQL or secrets; Databricks failures map to user-safe messages.

## Testing
- Install dev deps: `pip install -e .[test]`
- Run unit tests: `pytest`

## Migration from Data-Retrieval Version

If you were using the previous version that supported `sample_data`, `preview_query`, and `run_query` (which returned raw data), note these changes:

- **Removed**: `sample_data`, `preview_query`, `run_query` - all raw data retrieval tools
- **Removed**: Job execution tools (`submit_python_job`, `submit_notebook_job`, etc.)
- **Removed**: `jobs.py` module entirely
- **Added**: `approx_count` - get audience size without data
- **Added**: `aggregate_metric` - calculate metrics (COUNT, SUM, AVG, MIN, MAX) for audience analysis
- **Retained**: All metadata tools for schema exploration

The new version prioritizes **privacy** and **audience sizing** for lead generation use cases over raw data access.
