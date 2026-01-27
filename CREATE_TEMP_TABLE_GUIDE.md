# create_temp_table Tool Guide

## Overview

The `create_temp_table` tool creates session-scoped temporary views in Databricks from SELECT queries. This enables AI agents to create aggregated views that combine multiple tables or data sources for lead generation and analysis purposes without exposing raw data.

## Purpose

This tool is designed for lead generation scenarios where you need to:
- Combine data from multiple tables using JOINs
- Create aggregated views with GROUP BY, COUNT, SUM, AVG, etc.
- Build complex analysis views that can be queried later in the session
- Generate lead segments based on multiple criteria across different data sources

## Privacy-First Design

The tool maintains the privacy-first architecture by:
- **Only allowing SELECT statements** - No data modification
- **Validating all catalog/schema references** - Only allowlisted data sources
- **Session-scoped temporary views** - Automatically cleaned up, no persistent storage
- **Aggregate data focus** - Designed for summary metrics, not raw data exposure
- **Query pattern validation** - Blocks dangerous patterns like `INTO OUTFILE`, `EXEC`, etc.

## Parameters

### `temp_table_name` (required)
- **Type**: string
- **Description**: Name for the temporary view
- **Validation**: Must be a valid SQL identifier (alphanumeric and underscores only, must start with letter or underscore)
- **Example**: `"lead_segments"`, `"customer_summary"`, `"active_companies"`

### `sql_query` (required)
- **Type**: string
- **Description**: SELECT query to populate the temporary view
- **Requirements**:
  - Must be a SELECT statement
  - All referenced catalogs/schemas must be in the allowlist
  - Can include JOINs, aggregations, WHERE clauses, GROUP BY, etc.
  - Must not contain forbidden patterns (INTO OUTFILE, EXEC, CALL, etc.)
- **Format**: Standard SQL SELECT statement

### `request_id` (optional)
- **Type**: string
- **Description**: Request tracking ID for observability
- **Default**: Auto-generated UUID

## Returns

```json
{
  "temp_table_name": "lead_segments",
  "row_count": 150,
  "status": "created",
  "scope": "session",
  "note": "This temporary view is session-scoped and will be automatically cleaned up when the session ends"
}
```

## Usage Examples

### Example 1: Simple Aggregation

Create a temporary view summarizing customer orders:

```python
result = create_temp_table(
    temp_table_name="customer_summary",
    sql_query="""
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(order_amount) as total_spent,
            AVG(order_amount) as avg_order_value
        FROM `main`.`sales`.`orders`
        WHERE order_date >= '2024-01-01'
        GROUP BY customer_id
    """
)
```

### Example 2: Multi-Table JOIN

Create a lead generation view combining companies and contacts:

```python
result = create_temp_table(
    temp_table_name="qualified_leads",
    sql_query="""
        SELECT 
            c.id as company_id,
            c.name as company_name,
            c.industry,
            c.revenue,
            COUNT(DISTINCT ct.id) as contact_count,
            COUNT(DISTINCT t.id) as transaction_count,
            SUM(t.amount) as total_transaction_value
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        WHERE c.status = 'active'
            AND c.revenue > 1000000
        GROUP BY c.id, c.name, c.industry, c.revenue
        HAVING contact_count > 0
    """
)
```

### Example 3: Cross-Catalog Analysis

Combine operational and analytics data:

```python
result = create_temp_table(
    temp_table_name="campaign_effectiveness",
    sql_query="""
        SELECT 
            c.campaign_name,
            c.campaign_type,
            COUNT(DISTINCT l.id) as lead_count,
            COUNT(DISTINCT o.id) as conversion_count,
            SUM(o.amount) as revenue_generated,
            a.engagement_score
        FROM `main`.`marketing`.`campaigns` c
        LEFT JOIN `main`.`sales`.`leads` l ON c.id = l.campaign_id
        LEFT JOIN `main`.`sales`.`orders` o ON l.id = o.lead_id
        LEFT JOIN `analytics`.`reporting`.`campaign_metrics` a ON c.id = a.campaign_id
        WHERE c.start_date >= '2024-01-01'
        GROUP BY c.campaign_name, c.campaign_type, a.engagement_score
    """
)
```

### Example 4: Lead Scoring

Create a scored lead list based on multiple factors:

```python
result = create_temp_table(
    temp_table_name="scored_leads",
    sql_query="""
        SELECT 
            c.id,
            c.name,
            c.industry,
            CASE 
                WHEN c.revenue > 10000000 THEN 10
                WHEN c.revenue > 5000000 THEN 7
                WHEN c.revenue > 1000000 THEN 5
                ELSE 3
            END as revenue_score,
            CASE 
                WHEN COUNT(DISTINCT ct.id) > 10 THEN 10
                WHEN COUNT(DISTINCT ct.id) > 5 THEN 7
                WHEN COUNT(DISTINCT ct.id) > 2 THEN 5
                ELSE 3
            END as contact_score,
            CASE 
                WHEN SUM(t.amount) > 500000 THEN 10
                WHEN SUM(t.amount) > 100000 THEN 7
                WHEN SUM(t.amount) > 10000 THEN 5
                ELSE 3
            END as transaction_score
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        GROUP BY c.id, c.name, c.industry, c.revenue
    """
)
```

## Guardrails and Validation

### Allowed Patterns
✅ SELECT statements with JOINs  
✅ Aggregations (COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING)  
✅ Filtering (WHERE clauses)  
✅ Subqueries in SELECT statements  
✅ CASE statements  
✅ Multiple catalog/schema references (if all are allowlisted)  
✅ CTEs (Common Table Expressions)  

### Blocked Patterns
❌ Non-SELECT statements (INSERT, UPDATE, DELETE, DROP)  
❌ INTO OUTFILE / INTO DUMPFILE  
❌ EXEC / EXECUTE / CALL statements  
❌ LOAD_FILE operations  
❌ References to non-allowlisted catalogs or schemas  

### Validation Flow

1. **Identifier Validation**: Temp table name must be a valid SQL identifier
2. **Query Type Check**: Query must start with SELECT
3. **Pattern Scanning**: Check for forbidden patterns
4. **Catalog/Schema Validation**: Extract all catalog.schema.table references and validate against allowlist
5. **Execution**: Create temporary view with validated query
6. **Metadata Return**: Return temp table name and row count

## Error Handling

### GuardrailError
Raised when query violates guardrails:
```
GuardrailError: Temporary table query must be a SELECT statement for privacy-first design
GuardrailError: Query references catalog 'forbidden' which is not in allowlist
GuardrailError: Query contains forbidden pattern: \bINTO\s+OUTFILE\b
GuardrailError: Invalid identifier for temp_table_name
```

### QueryError
Raised when query execution fails:
```
QueryError: Failed to create temporary table 'lead_segments': [Databricks error details]
```

## Best Practices

### 1. Use Descriptive Names
```python
# Good
temp_table_name="high_value_leads_2024"

# Less descriptive
temp_table_name="temp1"
```

### 2. Include Aggregations
Temporary tables should focus on aggregated data, not raw records:
```python
# Good - Aggregated data
sql_query="""
    SELECT industry, COUNT(*) as company_count, AVG(revenue) as avg_revenue
    FROM companies
    GROUP BY industry
"""

# Avoid - Raw data exposure (though still allowed by guardrails)
sql_query="SELECT * FROM companies"
```

### 3. Filter Early
Apply WHERE clauses to reduce data volume:
```python
sql_query="""
    SELECT ...
    FROM companies
    WHERE status = 'active' AND created_date >= '2024-01-01'
    GROUP BY ...
"""
```

### 4. Use Request IDs for Tracking
```python
result = create_temp_table(
    temp_table_name="leads",
    sql_query="...",
    request_id="lead-gen-2024-q1"
)
```

### 5. Handle Errors Gracefully
```python
try:
    result = create_temp_table(
        temp_table_name="complex_view",
        sql_query=complex_query
    )
    print(f"Created view with {result['row_count']} rows")
except GuardrailError as e:
    print(f"Query validation failed: {e}")
except QueryError as e:
    print(f"Query execution failed: {e}")
```

## Performance Considerations

1. **Row Count Impact**: The tool queries the temp table for row count after creation. For very large result sets, this adds overhead.

2. **Session Lifetime**: Temporary views exist for the session duration. If creating many temp tables, monitor session resource usage.

3. **Concurrent Queries**: Respects `max_concurrent_queries` limit from configuration.

4. **Query Timeout**: Subject to `query_timeout_seconds` configured limit (default 60s).

## Configuration Requirements

To use `create_temp_table`, ensure your `config.yml` includes:

```yaml
limits:
  allow_statement_types:
    - SELECT
    - CREATE  # Required for CREATE TEMPORARY VIEW
```

## Security Notes

1. **No Persistent Storage**: Temporary views are session-scoped and never persisted to disk
2. **Automatic Cleanup**: Views are automatically dropped when the session ends
3. **Scope Enforcement**: All referenced tables must be in allowlisted catalogs/schemas
4. **No Data Exfiltration**: Patterns like INTO OUTFILE are explicitly blocked
5. **Audit Trail**: All operations are logged with request IDs for tracking

## Troubleshooting

### "Invalid identifier for temp_table_name"
- Use only alphanumeric characters and underscores
- Must start with a letter or underscore
- Example: `lead_summary_2024` ✅, `2024-leads` ❌

### "Query must be a SELECT statement"
- Query must start with SELECT
- No INSERT, UPDATE, DELETE, or other DML/DDL statements

### "Catalog X is not in allowlist"
- All catalogs referenced in the query must be configured in `scopes.catalogs`
- Check your config.yml

### "Query contains forbidden pattern"
- Review query for blocked patterns (INTO OUTFILE, EXEC, CALL, etc.)
- Rewrite query to avoid these patterns

### "Failed to create temporary table"
- Check Databricks SQL syntax
- Verify table/column names exist
- Check warehouse connectivity
- Review Databricks error message in logs
