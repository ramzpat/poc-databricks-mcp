# Static Metadata Directory

This directory contains static metadata files for Databricks tables. The metadata is used to provide additional column definitions, descriptions, and business context without querying Databricks directly.

## Directory Structure

```
metadata/
├── README.md                          # This file
├── <catalog>/                         # Catalog name
│   └── <schema>/                      # Schema name
│       └── <table>.csv                # Table metadata file
└── examples/                          # Example metadata files
    └── main/
        └── default/
            └── sample_table.csv       # Example table metadata
```

## Metadata File Format (CSV)

Each table should have a CSV file with the following columns:

| Column Name         | Required | Description                                    |
|---------------------|----------|------------------------------------------------|
| column_name         | Yes      | Name of the column                             |
| data_type           | No       | Data type of the column                        |
| description         | No       | Detailed description of the column             |
| business_definition | No       | Business context and meaning                   |
| example_values      | No       | Example values for the column                  |
| constraints         | No       | Any constraints (e.g., "NOT NULL", "UNIQUE")   |
| source_system       | No       | Source system or upstream table                |
| owner               | No       | Data owner or team responsible                 |
| tags                | No       | Comma-separated tags for categorization        |

### Example CSV File

```csv
column_name,data_type,description,business_definition,example_values,constraints,source_system,owner,tags
user_id,BIGINT,Unique identifier for users,Primary key for user records in the system,"12345, 67890",NOT NULL PRIMARY KEY,CRM System,Data Engineering,pii,key
username,STRING,User's login name,Unique username for authentication,"john_doe, jane_smith",NOT NULL UNIQUE,CRM System,Data Engineering,pii
email,STRING,User's email address,Primary contact email for communications,"user@example.com",NOT NULL,CRM System,Data Engineering,pii,contact
created_at,TIMESTAMP,Account creation timestamp,When the user account was first created,"2024-01-15 10:30:00",NOT NULL,CRM System,Data Engineering,audit
is_active,BOOLEAN,Active status flag,Indicates if the user account is currently active,"true, false",NOT NULL DEFAULT true,CRM System,Data Engineering,status
```

## Usage

1. **Create metadata files**: Place CSV files in the appropriate catalog/schema directory structure
2. **Configure**: Set the `metadata_directory` in your `config.yml`:
   ```yaml
   metadata:
     enabled: true
     directory: ./metadata
   ```
3. **Access metadata**: Use the MCP tools to retrieve metadata. The tools will automatically merge static metadata with live Databricks metadata.

## Best Practices

1. **Keep files up to date**: Update metadata files when schema changes occur
2. **Use consistent naming**: Match catalog, schema, and table names exactly as they appear in Databricks
3. **Provide meaningful descriptions**: Focus on business context that isn't available in Databricks
4. **Version control**: Keep metadata files in version control alongside your code
5. **Review regularly**: Establish a process to review and update metadata periodically

## Integration with MCP Tools

The following MCP tools use static metadata:

- `table_metadata`: Merges static metadata with live Databricks metadata
- `get_static_metadata`: Returns only the static metadata (new tool)

Static metadata takes precedence for the following fields when merging:
- `description`
- `business_definition`
- `example_values`
- `constraints`
- `source_system`
- `owner`
- `tags`

Databricks metadata is used for technical details like `data_type`, `nullable`, and `ordinal_position`.
