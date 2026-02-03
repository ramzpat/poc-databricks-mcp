# Static Metadata Usage Guide

This guide demonstrates how to use the static metadata feature to enrich your Databricks table metadata with business context and documentation.

## Quick Start

### 1. Enable Static Metadata

Edit your `config.yml`:

```yaml
metadata:
  enabled: true
  directory: ./metadata
```

### 2. Create Metadata Files

Create a CSV file for your table at `metadata/<catalog>/<schema>/<table>.csv`:

```bash
mkdir -p metadata/main/default
```

Create `metadata/main/default/users.csv`:

```csv
column_name,data_type,description,business_definition,example_values,constraints,source_system,owner,tags
user_id,BIGINT,Unique identifier for users,Primary key for user records in the system,"12345, 67890",NOT NULL PRIMARY KEY,CRM System,Data Engineering,pii,key
username,STRING,User's login name,Unique username for authentication,"john_doe, jane_smith",NOT NULL UNIQUE,CRM System,Data Engineering,pii
email,STRING,User's email address,Primary contact email for communications,"user@example.com",NOT NULL,CRM System,Data Engineering,pii,contact
created_at,TIMESTAMP,Account creation timestamp,When the user account was first created,"2024-01-15 10:30:00",NOT NULL,CRM System,Data Engineering,audit
status,STRING,Account status,Current status of the user account,"active, suspended, deleted",NOT NULL,CRM System,Product Team,status
```

### 3. Use the Enhanced Tools

The metadata is automatically merged when you call `table_metadata`:

```python
# The table_metadata tool now returns enriched information
result = await table_metadata(
    catalog="main",
    schema="default",
    table="users"
)

# Result includes both Databricks metadata and your static metadata:
# {
#   "catalog": "main",
#   "schema": "default",
#   "table": "users",
#   "columns": [
#     {
#       "name": "user_id",
#       "data_type": "BIGINT",           # From Databricks
#       "nullable": "NO",                 # From Databricks
#       "description": "Unique identifier for users",  # From CSV
#       "business_definition": "Primary key for user records",  # From CSV
#       "example_values": "12345, 67890",  # From CSV
#       "constraints": "NOT NULL PRIMARY KEY",  # From CSV
#       "source_system": "CRM System",    # From CSV
#       "owner": "Data Engineering",      # From CSV
#       "tags": "pii,key"                # From CSV
#     },
#     ...
#   ],
#   "has_static_metadata": true
# }
```

You can also retrieve only the static metadata without querying Databricks:

```python
# Get static metadata only (no Databricks query)
result = await get_static_metadata(
    catalog="main",
    schema="default",
    table="users"
)

# Result:
# {
#   "enabled": true,
#   "metadata": [
#     {
#       "column_name": "user_id",
#       "description": "Unique identifier for users",
#       "business_definition": "Primary key for user records",
#       ...
#     },
#     ...
#   ],
#   "message": "Found 5 column metadata entries"
# }
```

## Use Cases

### 1. Data Discovery and Documentation

Provide comprehensive documentation for data analysts and scientists:

```csv
column_name,description,business_definition,tags
customer_id,Unique customer identifier,Primary key linking to customer dimension table,pii,key,dimension
order_date,Date the order was placed,Business date when customer submitted the order (not processing date),temporal,business_date
total_amount,Total order amount in USD,Sum of all line items including tax and shipping fees,financial,currency_usd
```

### 2. Data Governance and Compliance

Track data ownership and compliance requirements:

```csv
column_name,description,owner,tags,constraints
ssn,Social Security Number,Legal Team,pii,gdpr,hipaa,NOT NULL ENCRYPTED
credit_card_number,Customer credit card,Payment Team,pii,pci_dss,NOT NULL MASKED
email,Customer email address,Marketing Team,pii,gdpr,NOT NULL
```

### 3. Data Quality and Validation

Document expected values and constraints:

```csv
column_name,description,example_values,constraints,business_definition
order_status,Current order status,"pending, processing, shipped, delivered, cancelled",ENUM CHECK,Must be one of the predefined status values
price,Product price in cents,"999, 1499, 2999",CHECK price > 0,Always stored as integer cents to avoid floating point issues
country_code,ISO country code,"US, GB, DE, FR",CHECK LENGTH = 2,Two-letter ISO 3166-1 alpha-2 code
```

### 4. Source System Tracking

Track data lineage and source systems:

```csv
column_name,description,source_system,business_definition
customer_name,Customer full name,Salesforce CRM,Synced daily from Salesforce Contact.Name field
order_date,Order placement date,E-commerce Platform,Real-time stream from orders table
inventory_count,Current inventory level,WMS (Warehouse Management),Updated every 15 minutes via batch sync
```

## Best Practices

### Directory Structure

Organize metadata files to match your Databricks structure:

```
metadata/
├── production/           # Production catalog
│   ├── sales/
│   │   ├── orders.csv
│   │   ├── customers.csv
│   │   └── products.csv
│   └── analytics/
│       ├── daily_metrics.csv
│       └── user_segments.csv
├── staging/              # Staging catalog
│   └── raw/
│       ├── events.csv
│       └── logs.csv
└── examples/            # Example metadata (for documentation)
    └── main/
        └── default/
            └── sample_table.csv
```

### CSV Format Guidelines

1. **Always include column_name**: This is the only required field
2. **Be consistent**: Use the same format across all metadata files
3. **Use clear descriptions**: Write for humans, not machines
4. **Leverage tags**: Use comma-separated tags for categorization
5. **Keep it updated**: Review and update metadata when schemas change

### Version Control

Keep metadata files in version control:

```bash
# Add to git
git add metadata/
git commit -m "Add metadata for users table"

# Review changes
git diff metadata/production/sales/orders.csv
```

### Validation Workflow

1. Create/update metadata CSV files
2. Test locally with `get_static_metadata` tool
3. Verify merging with `table_metadata` tool
4. Commit to version control
5. Deploy configuration updates

## Troubleshooting

### Metadata not loading

Check the configuration:

```yaml
metadata:
  enabled: true  # Must be true
  directory: ./metadata  # Path must exist
```

Verify the file path matches exactly:
```bash
# For catalog=main, schema=default, table=users
# File must be at: metadata/main/default/users.csv
ls -la metadata/main/default/users.csv
```

### Metadata not merging

- Column names in CSV must match Databricks column names (case-insensitive)
- CSV must be valid (no syntax errors)
- Check logs for loading errors

### Performance considerations

- Metadata is cached after first load (per table)
- No performance impact on queries - only affects metadata retrieval
- CSV files should be reasonable size (< 1MB recommended)

## Advanced Features

### Programmatic Access

Access metadata programmatically:

```python
from databricks_mcp.metadata_loader import MetadataLoader

# Initialize loader
loader = MetadataLoader(
    metadata_dir="./metadata",
    enabled=True
)

# Load metadata
metadata = loader.get_table_metadata("main", "default", "users")

# Clear cache to reload
loader.clear_cache()
```

### Custom Metadata Fields

Add your own fields to the CSV as needed:

```csv
column_name,description,custom_field_1,custom_field_2,my_special_flag
user_id,User identifier,value1,value2,yes
```

All fields are preserved and returned in the metadata response.

## Migration from Documentation Tools

If you're currently using other documentation tools:

1. Export metadata to CSV format
2. Map fields to the supported columns
3. Place files in the correct directory structure
4. Enable metadata in config
5. Verify with `get_static_metadata` tool

## Support

For issues or questions:
- Check `metadata/README.md` for format specification
- Review example files in `metadata/examples/`
- See test files in `tests/test_metadata_loader.py`
