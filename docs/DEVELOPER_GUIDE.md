# Databricks MCP Server - Developer Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Quick Start](#quick-start)
5. [Template Adaptation](#template-adaptation)
6. [Adding New Tools](#adding-new-tools)
7. [Static Metadata Feature](#static-metadata-feature)
8. [Code Examples](#code-examples)

---

## Overview

This project implements a Databricks MCP (Model Context Protocol) server using the official [mcp-server-hello-world template](https://github.com/databricks/app-templates/tree/main/mcp-server-hello-world) pattern. It provides AI assistants with tools to query and analyze data in Databricks.

### Key Features

- ✓ FastMCP server with FastAPI integration
- ✓ HTTP streaming support via uvicorn
- ✓ SQL guardrails for query safety
- ✓ Partition analysis and data profiling
- ✓ OAuth token provider for workspace auth
- ✓ Configuration management via YAML
- ✓ Databricks Apps deployment ready
- ✓ Static metadata ingestion from CSV files

---

## Architecture

### Before Adaptation (Monolithic)
```
server.py
  └─ build_app(config, sql_client, jobs_client)
     └─ FastMCP with inline @app.tool() decorators
```

### After Adaptation (Modular - Template Pattern)
```
src/databricks_mcp/
  ├── server.py      - Main entry point and FastAPI/FastMCP combined application
  ├── auth/          - Authentication Logic
  │   └── dependencies.py - OAuth token provider
  ├── db/            - Database / SQL Client
  │   ├── client.py  - SQL client
  │   └── models.py  - Pydantic models (placeholder)
  ├── guardrails/    - Safety & Validation
  │   └── input_checks.py - Query validation, scope checks
  └── tools/         - MCP Tool definitions
      ├── data_tools.py - Data analysis tools
      └── user_tools.py - User tools (placeholder)

Dependencies:
  ├── config.py      - Configuration management
  ├── config.py      - Configuration
  └── [other modules]
```

---

## Project Structure

```
.
├── src/databricks_mcp/
│   ├── server.py                  # Main entry point and FastAPI/FastMCP setup
│   ├── auth/                      # Authentication Logic
│   │   ├── __init__.py
│   │   └── dependencies.py        # OAuth provider
│   ├── db/                        # Database / SQL Client
│   │   ├── __init__.py
│   │   ├── client.py              # SQL client
│   │   └── models.py              # Pydantic models (placeholder)
│   ├── guardrails/                # Safety & Validation
│   │   ├── __init__.py
│   │   └── input_checks.py        # Query validation
│   ├── tools/                     # MCP Tool definitions
│   │   ├── __init__.py
│   │   ├── data_tools.py          # Data analysis tools
│   │   └── user_tools.py          # User tools (placeholder)
│   ├── config.py                  # Config management
│   ├── errors.py                  # Custom exceptions
│   └── logging_utils.py           # Logging setup
├── docs/                          # Documentation
│   └── DEVELOPER_GUIDE.md         # This file
├── pyproject.toml                 # Project dependencies
├── app.yaml                       # Databricks Apps config
├── config.example.yml             # Configuration template
├── README.md                     # Main documentation
└── requirements.txt              # Python dependencies
```

---

## Quick Start

### 1. Install Dependencies
```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### 2. Run Locally
```bash
# Default port (8000)
uv run custom-server

# Custom port
uv run custom-server --port 8080
```

### 3. Test MCP Endpoints
```bash
# Health check
curl http://localhost:8000/

# Query tools
curl http://localhost:8000/mcp
```

### 4. Deploy to Databricks Apps
```bash
databricks apps deploy
```

---

## Template Adaptation

### What Changed

#### 1. **Dependencies** (pyproject.toml)
```python
dependencies = [
    "fastapi>=0.115.12",      # NEW
    "mcp[cli]>=1.14.0",       # NEW
    "uvicorn>=0.34.2",        # NEW
    "databricks-sdk>=0.60.0",
    "pydantic>=2",
    "fastmcp>=2.12.5",        # UPDATED (was >=0.4.0)
    # ... other dependencies
]
```

#### 2. **New Server Structure**

**server.py** - Main entry point with FastAPI/FastMCP combination
```python
import argparse
import uvicorn

def main() -> None:
    parser = argparse.ArgumentParser(description="Start the Databricks MCP server")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server")
    args = parser.parse_args()
    
    uvicorn.run(
        "databricks_mcp.server:combined_app",
        host="0.0.0.0",
        port=args.port,
    )

def create_app(config_path: Path | None = None):
    # Load config and initialize clients
    config = load_config(config_path or _config_path())
    token_provider = OAuthTokenProvider(config.oauth)
    sql_client = DatabricksSQLClient(config, token_provider)
    jobs_client = DatabricksJobsClient(config, token_provider)
    
    # Create FastMCP instance and load tools
    mcp_server = FastMCP(name="databricks-mcp")
    load_tools(mcp_server, sql_client, jobs_client)
    mcp_app = mcp_server.http_app()
    
    # Combine MCP routes with FastAPI routes
    combined_app = FastAPI(
        title="Databricks MCP App",
        routes=[*mcp_app.routes, *app.routes],
        lifespan=mcp_app.lifespan,
    )
    
    # Middleware for auth header capture
    @combined_app.middleware("http")
    async def capture_headers(request: Request, call_next):
        header_store.set(dict(request.headers))
        return await call_next(request)
    
    return combined_app, sql_client, jobs_client

combined_app, _sql_client, _jobs_client = create_app()
```

**tools/data_tools.py** - Tool registration for data analysis
```python
def register_data_tools(mcp_server, sql_client):
    @mcp_server.tool()
    async def list_catalogs(request_id: str | None = None) -> list[str]:
        """List all available catalogs in the Databricks workspace."""
        rid = _request_id(request_id)
        return await asyncio.to_thread(sql_client.list_catalogs, rid)
    
    # ... more tools ...
```
    return WorkspaceClient()

def get_user_authenticated_workspace_client() -> WorkspaceClient:
    """End-user authenticated client (for Databricks Apps)."""
    return WorkspaceClient()
```

#### 3. **Module Organization**
The code is now organized into focused packages:
```python
# auth/ - Authentication logic
from databricks_mcp.auth import OAuthTokenProvider

# db/ - Database operations
from databricks_mcp.db import DatabricksSQLClient

# guardrails/ - Safety checks
from databricks_mcp.guardrails import sanitize_identifier

# tools/ - MCP tool registration
from databricks_mcp.tools import register_data_tools
```

### Why This Pattern?

| Feature | Before | After |
|---------|--------|-------|
| Modularity | Single file | Separated: server.py, auth/, db/, tools/ |
| Adding tools | Modify server.py | Add to tools/data_tools.py |
| Port config | Code change needed | CLI: `--port 8080` |
| HTTP support | Basic MCP | Full FastAPI + streaming |
| Middleware | Limited | Full FastAPI support |
| Static files | Not supported | Built-in serving |
| Testing | Monolithic | Unit test each module |
| Production | Partial | Complete |

---

## Adding New Tools

### Method 1: Simple Tool
```python
# In tools/data_tools.py, inside register_data_tools() function
@mcp_server.tool()
async def my_tool(param: str) -> dict:
    """
    Brief description for AI assistants.
    
    Args:
        param: Parameter description
    
    Returns:
        dict: Result description
    """
    result = process(param)
    return {"result": result}
```

### Method 2: Tool with SQL Client
```python
@mcp_server.tool()
async def analyze_data(catalog: str, schema: str, table: str) -> dict:
    """Analyze table data for insights."""
    metadata = await asyncio.to_thread(
        sql_client.table_metadata,
        catalog,
        schema,
        table,
        _request_id()
    )
    return metadata
```

### Method 3: Tool with Error Handling
```python
@mcp_server.tool()
async def safe_query(sql: str, limit: int = 100) -> dict:
    """Execute query with validation."""
    try:
        result = await asyncio.to_thread(
            sql_client.run_query,
            sql,
            limit,
            timeout_seconds=30,
            request_id=_request_id()
        )
        return result
    except Exception as e:
        return {"error": str(e), "message": "Query execution failed"}
```

### Tool Best Practices

1. **Clear naming**: Use descriptive, action-oriented names
   - ✓ `list_catalogs()`, `sample_data()`
   - ✗ `get_stuff()`, `process()`

2. **Comprehensive docstrings**: AI assistants use these to decide when to call
   ```python
   @mcp_server.tool()
   async def my_tool(param: str) -> dict:
       """
       What this tool does in plain language.
       
       This is shown to AI when deciding whether to use the tool.
       Include context about when and why to use it.
       
       Args:
           param: Detailed parameter description
       
       Returns:
           dict: What structure you return
       """
   ```

3. **Type hints**: Help with validation and documentation
   ```python
   async def my_tool(
       catalog: str,           # String parameter
       limit: int | None = 100 # Optional integer
   ) -> dict[str, Any]:        # Returns dict
   ```

4. **Async execution**: Use `asyncio.to_thread()` for blocking operations
   ```python
   result = await asyncio.to_thread(sql_client.run_query, sql)
   ```

5. **Error handling**: Return structured error information
   ```python
   try:
       result = await asyncio.to_thread(dangerous_op)
       return {"success": True, "data": result}
   except Exception as e:
       return {"success": False, "error": str(e)}
   ```

---

## Static Metadata Feature

The server supports static metadata ingestion from CSV files, allowing you to enrich table metadata with business context, column descriptions, and documentation without querying Databricks.

### Configuration

Add to your `config.yml`:

```yaml
metadata:
  enabled: true
  directory: ./metadata
```

### Directory Structure

Create metadata files following this structure:

```
metadata/
├── <catalog>/
│   └── <schema>/
│       └── <table>.csv
```

### CSV Format

Each CSV file should have these columns:

| Column              | Required | Description                          |
|---------------------|----------|--------------------------------------|
| column_name         | Yes      | Column name (matches Databricks)     |
| data_type           | No       | Data type                            |
| description         | No       | Column description                   |
| business_definition | No       | Business context                     |
| example_values      | No       | Example values                       |
| constraints         | No       | Constraints (NOT NULL, etc.)         |
| source_system       | No       | Source system                        |
| owner               | No       | Data owner                           |
| tags                | No       | Comma-separated tags                 |

### Example

`metadata/main/default/users.csv`:

```csv
column_name,description,business_definition,owner,tags
user_id,Unique user identifier,Primary key for user records,Data Engineering,pii,key
username,Login username,Unique username for authentication,Data Engineering,pii
email,User email address,Primary contact email,Marketing,pii,contact
```

### Integration

#### MetadataLoader Class

The `MetadataLoader` class in `src/databricks_mcp/metadata_loader.py` handles:
- Loading CSV files
- Caching metadata
- Merging with Databricks metadata

```python
from databricks_mcp.metadata_loader import MetadataLoader, merge_metadata

# Initialize
loader = MetadataLoader(
    metadata_dir="./metadata",
    enabled=True
)

# Load metadata
metadata = loader.get_table_metadata("catalog", "schema", "table")

# Merge with Databricks columns
merged = merge_metadata(databricks_columns, metadata)
```

#### DatabricksSQLClient Integration

The client automatically merges static metadata:

```python
# In db/client.py
def table_metadata(self, catalog, schema, table, request_id=None):
    # ... fetch from Databricks ...
    
    # Load static metadata
    static_metadata = self._metadata_loader.get_table_metadata(
        catalog, schema, table
    )
    
    # Merge
    merged_columns = merge_metadata(databricks_columns, static_metadata)
    
    return {
        "columns": merged_columns,
        "has_static_metadata": static_metadata is not None
    }
```

### MCP Tools

#### table_metadata

Enhanced to include static metadata:

```python
@mcp_server.tool()
async def table_metadata(catalog: str, schema: str, table: str):
    """Get table metadata with static metadata merged in."""
    return await sql_client.table_metadata(catalog, schema, table)
```

#### get_static_metadata

New tool to get only static metadata:

```python
@mcp_server.tool()
async def get_static_metadata(catalog: str, schema: str, table: str):
    """Get static metadata without querying Databricks."""
    return await sql_client.get_static_metadata(catalog, schema, table)
```

### Testing

Tests are in `tests/test_metadata_loader.py`:

```python
def test_metadata_loader_load_existing(metadata_dir):
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    metadata = loader.get_table_metadata("catalog", "schema", "table")
    assert metadata is not None
    assert len(metadata) > 0

def test_merge_metadata_with_static():
    databricks_columns = [{"name": "col1", "data_type": "STRING"}]
    static_metadata = [{"column_name": "col1", "description": "Test"}]
    merged = merge_metadata(databricks_columns, static_metadata)
    assert merged[0]["description"] == "Test"
```

Run tests:

```bash
pytest tests/test_metadata_loader.py -v
```

### Best Practices

1. **Keep metadata files in version control**
2. **Use consistent naming** matching Databricks exactly
3. **Focus on business context** not available in Databricks
4. **Update when schemas change**
5. **Use tags for categorization** (pii, key, audit, etc.)

### See Also

- `metadata/README.md` - Detailed format specification
- `docs/STATIC_METADATA_GUIDE.md` - Complete usage guide
- `metadata/examples/` - Example CSV files

---

## Code Examples

### Running with Different Ports

```bash
# Development (default)
uv run custom-server
# Server available at http://localhost:8000

# Testing alternative port
uv run custom-server --port 8081

# Production
uv run custom-server --port 3000
```

### Querying Tools Locally

```python
# Python script to test locally
import requests

BASE_URL = "http://localhost:8000"

# Get tool list
response = requests.get(f"{BASE_URL}/mcp")
tools = response.json()

# Query specific tool
data = {
    "method": "tools/call",
    "params": {
        "name": "list_catalogs",
        "arguments": {}
    }
}
response = requests.post(f"{BASE_URL}/mcp", json=data)
```

### Testing Deployed App

```bash
# Query deployed MCP server with OAuth
python scripts/dev/query_remote.py \
    --host "https://your-workspace.cloud.databricks.com" \
    --token "your-oauth-token" \
    --app-url "https://your-workspace.cloud.databricks.com/serving-endpoints/your-app"
```

### Custom API Endpoint (Beyond MCP Tools)

```python
# In server.py, add to the FastAPI app
@app.get("/health")
async def health_check():
    """Custom health check endpoint."""
    return {"status": "healthy", "service": "databricks-mcp"}

@app.post("/query")
async def custom_query(request: QueryRequest):
    """Custom endpoint not exposed via MCP."""
    sql_client = get_sql_client()
    result = await asyncio.to_thread(sql_client.run_query, request.sql)
    return result
```

---

## Common Tasks

### Deploy to Databricks Apps
```bash
# Ensure config.yml exists
cp config.example.yml config.yml

# Edit config.yml with your workspace settings
nano config.yml

# Deploy
databricks apps deploy
```

### Check App Logs
```bash
databricks apps logs poc-databricks-mcp --tail-lines 100 --profile poc
```

### Format Code
```bash
# Format with ruff
uv run ruff format .

# Check for lint issues
uv run ruff check .
```

### Run Tests
```bash
# Run all tests
uv run pytest tests/

# Run specific test
uv run pytest tests/test_config.py -v
```

---

## Troubleshooting

### Port Already in Use
```bash
# Find process using port 8000
lsof -i :8000

# Kill process
kill -9 <PID>

# Or use different port
uv run custom-server --port 8081
```

### Import Errors
```bash
# Reinstall dependencies
uv sync

# Or
pip install -r requirements.txt
```

### Configuration Issues
```bash
# Verify config.yml exists and is valid
cat config.yml

# Check environment variables
echo $DATABRICKS_HOST
echo $DATABRICKS_HTTP_PATH
```

### Authentication Failures
```bash
# Verify OAuth credentials
cat config.yml | grep -A 5 oauth

# Test Databricks connection
python -c "from databricks.sdk import WorkspaceClient; w = WorkspaceClient(); print(w.current_user.me())"
```

---

## Resources

- [Databricks MCP Documentation](https://docs.databricks.com/aws/en/generative-ai/mcp/custom-mcp)
- [FastMCP GitHub](https://github.com/jlowin/fastmcp)
- [Model Context Protocol Spec](https://modelcontextprotocol.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Uvicorn Documentation](https://www.uvicorn.org/)

---

## Next Steps

1. **Install dependencies**: `uv sync`
2. **Configure**: Copy and edit `config.yml`
3. **Run locally**: `uv run custom-server`
4. **Add tools**: Extend `tools/data_tools.py`
5. **Test**: Use provided scripts
6. **Deploy**: `databricks apps deploy`

