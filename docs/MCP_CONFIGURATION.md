# Setting Up Your MCP Server Configuration

Your Databricks MCP server is now deployed and accessible at:
```
https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp
```

## Configuration for Claude Desktop

### Step 1: Locate the Config File

**macOS:**
```bash
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Linux:**
```bash
~/.config/Claude/claude_desktop_config.json
```

**Windows:**
```
%APPDATA%\Claude\claude_desktop_config.json
```

### Step 2: Add Your Server Configuration

Edit the config file and add your MCP server in the `mcpServers` section:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "url": "https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp"
    }
  }
}
```

### Complete Example Config

If the file doesn't exist, create it with this template:

```json
{
  "mcpServers": {
    "databricks-mcp": {
      "url": "https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp",
      "disabled": false
    }
  }
}
```

### Step 3: Restart Claude Desktop

After updating the config:
1. Quit Claude Desktop completely
2. Reopen Claude Desktop
3. The MCP server should now be available

## Troubleshooting

### Check if Server is Connected
In Claude Desktop, look for the MCP server indicator (usually a tool icon) in the UI.

### Server Not Appearing
- Verify the URL is correct
- Check that the Databricks app is running: `databricks apps logs poc-databricks-mcp --profile poc`
- Ensure JSON is valid (use a JSON validator)
- Restart Claude Desktop

### Test the Server Directly

```bash
# Check if the server is responding
curl https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp

# Should return MCP protocol response
```

## Using with Other MCP Clients

### Generic HTTP-based MCP Client

```python
import requests

MCP_URL = "https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp"

# List available tools
response = requests.post(
    f"{MCP_URL}",
    json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    }
)
print(response.json())
```

### Node.js / JavaScript Client

```javascript
const MCP_URL = "https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp";

const response = await fetch(MCP_URL, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "tools/list",
    params: {}
  })
});

console.log(await response.json());
```

## Authentication

Your Databricks MCP server supports OAuth authentication through request headers. When deployed in Databricks Apps, authentication is handled automatically by the app platform.

For custom clients, include Databricks authentication headers:

```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://poc-databricks-mcp-123360762620797.gcp.databricksapps.com/mcp
```

## Available Tools

Once connected, you can use these tools:

- `list_catalogs()` - List all Databricks catalogs
- `list_schemas(catalog)` - List schemas in a catalog
- `list_tables(catalog, schema)` - List tables in a schema
- `table_metadata(catalog, schema, table)` - Get table metadata
- `partition_info(catalog, schema, table)` - Get partition information
- `sample_data(catalog, schema, table)` - Get sample data
- `preview_query(sql, limit)` - Preview query results
- `run_query(sql, limit)` - Execute a query

## Configuration Reference

| Field | Description | Required |
|-------|-------------|----------|
| `url` | Full URL to your MCP server endpoint | Yes |
| `disabled` | Set to `true` to disable the server | No |
| `timeout` | Request timeout in milliseconds | No |

