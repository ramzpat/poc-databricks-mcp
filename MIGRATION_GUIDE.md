---
title: Migration Guide - Lead Generation Edition
description: Changes and migration instructions for the Databricks MCP server redesign
---

# Migration Guide: Databricks MCP Lead Generation Edition

## Overview

The Databricks MCP server has been redesigned to focus on **privacy-safe lead generation and audience sizing** through aggregated metrics. This represents a significant change from the previous data-retrieval focused approach.

## Key Changes

### 1. Removed Functionality

#### Data Retrieval Tools (Removed)
- **`sample_data`**: Previously used to retrieve sample rows from tables
- **`preview_query`**: Previously used for limited query preview with raw results
- **`run_query`**: Previously used to execute arbitrary SQL and return results

These tools have been removed entirely to ensure **no raw data is ever exposed**.

#### Job Execution (Removed)
- **`submit_python_job`**: Python code execution via Databricks jobs
- **`submit_notebook_job`**: Notebook execution via Databricks jobs
- **`execute_python_code`**: Direct code execution
- **`get_job_status`**: Job status monitoring
- **`get_job_run_output`**: Job output retrieval
- **`cancel_run`**: Job cancellation

The entire `jobs.py` module has been removed. Job execution is no longer part of this MCP server's responsibilities.

### 2. New Functionality

#### Lead Generation Tools (Added)

**`approx_count(catalog, schema, table, predicate?)`**
- Returns approximate row count for audience sizing
- Supports optional WHERE clause predicates
- No raw data returned, only aggregated count
- Perfect for "how many customers match these criteria?"

Example:
```python
approx_count(
    catalog="main",
    schema="leads",
    table="customer_360",
    predicate="age > 25 AND income_level = 'high'"
)
# Returns: {"approx_count": 15000, "predicate": "..."}
```

**`aggregate_metric(catalog, schema, table, metric_type, metric_column, predicate?)`**
- Calculates aggregated metrics (COUNT, SUM, AVG, MIN, MAX)
- No individual rows returned
- Supports filtering with predicates
- Examples: average customer lifetime value, total engaged users, etc.

Example:
```python
aggregate_metric(
    catalog="main",
    schema="leads",
    table="customer_360",
    metric_type="AVG",
    metric_column="customer_lifetime_value",
    predicate="location = 'NYC'"
)
# Returns: {"metric_value": 5200.50, "metric_type": "AVG", ...}
```

### 3. Retained Functionality

All metadata and schema exploration tools remain unchanged:

- **`list_catalogs`**: Explore available catalogs
- **`list_schemas`**: Browse schemas in a catalog
- **`list_tables`**: See tables and views in a schema
- **`table_metadata`**: Examine column structure and data types
- **`partition_info`**: Understand partitioning strategy

These tools allow users to explore the data model and build intelligent predicates for aggregation.

## File Structure Changes

### Removed Files
- `src/databricks_mcp/jobs.py` - Job execution client (completely removed)

### Modified Files
- `src/databricks_mcp/client.py`
  - Removed: `sample_data()`, `preview_query()`, `run_query()`, `_wrap_with_limit()`
  - Added: `approx_count()`, `aggregate_metric()`
  
- `src/databricks_mcp/server.py`
  - Removed: Import of `DatabricksJobsClient` and `jobs` module
  - Removed: All job-related tool decorators
  - Added: `approx_count()` tool decorator
  - Added: `aggregate_metric()` tool decorator
  - Updated: `main()` function to instantiate only `DatabricksSQLClient`

- `src/databricks_mcp/config.yml`
  - Added: Lead generation focus comment

### Updated Documentation
- `README.md` - Redesigned for lead generation focus
- `REQUIREMENTS.md` - Updated requirements for aggregation-focused approach
- `DATABRICKS_RESOURCES_SUMMARY.md` - Added aggregation use case section
- `pyproject.toml` - Updated description

## Migration Workflow

### For Users Previously Using Raw Data Tools

**Before (Data Retrieval):**
```python
# Get sample data
result = run_query(
    sql="SELECT * FROM customer_360 WHERE age > 25 LIMIT 100",
    limit=100
)
# Returns: rows with actual customer data
```

**After (Lead Generation):**
```python
# Get audience size
result = approx_count(
    catalog="main",
    schema="poc_mcp",
    table="vw_customer360",
    predicate="demo_age_fact_v2_final_age_num > 25"
)
# Returns: {"approx_count": 15000, "predicate": "..."}

# Then export conditions to generate Jupyter notebook for internal DS team
# to analyze actual data within Databricks
```

### New Workflow for Lead Generation

1. **Explore**: Use metadata tools to understand available columns and partitions
   ```python
   metadata = table_metadata("main", "poc_mcp", "vw_customer360")
   # Explore available columns for building predicates
   ```

2. **Build Conditions**: Define business logic predicates
   ```python
   predicate = "demo_age_fact_v2_final_age_num > 25 AND beha_mobile_usage_active_period_weekly_flag = 1"
   ```

3. **Size Audience**: Use aggregation tools to estimate audience
   ```python
   result = approx_count("main", "poc_mcp", "vw_customer360", predicate)
   # Audience size: 15,000 customers matching criteria
   ```

4. **Analyze Metrics**: Calculate aggregated statistics
   ```python
   revenue = aggregate_metric(
       catalog="main", schema="poc_mcp", table="customer_metrics",
       metric_type="SUM", metric_column="customer_lifetime_value",
       predicate=predicate
   )
   # Total LTV from audience: $78M
   ```

5. **Export & Deep Dive**: Export conditions to generate Jupyter notebook
   ```
   # (Future enhancement) Export conditions as parameters to generate 
   # a Jupyter notebook for internal DS team to analyze on Databricks
   ```

## Configuration Changes

No breaking configuration changes. The existing `config.yml` structure remains the same:

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
        - poc_mcp  # Lead generation tables
limits:
  max_rows: 10000          # Limit for aggregation result sets
  query_timeout_seconds: 60
  max_concurrent_queries: 5
  allow_statement_types:
    - SELECT               # Only SELECT allowed
```

## Database Design Considerations

### Optimizing for Aggregation

The new MCP server is optimized for aggregation queries. For best performance:

1. **Use Partitioned Tables**: Partition by date/region/segment to enable efficient filtering
2. **Pre-aggregate Where Possible**: Create materialized views with pre-calculated metrics
3. **Index Key Columns**: Ensure frequently-filtered columns are indexed
4. **Denormalize for Speed**: Flatten hierarchical data to speed up aggregations

### Query Patterns

The MCP server will generate queries like:

```sql
-- Approximate count with filtering
SELECT COUNT(*) as approx_count FROM catalog.schema.table WHERE predicate

-- Aggregated metrics
SELECT SUM(column) as metric_value FROM catalog.schema.table WHERE predicate
SELECT AVG(column) as metric_value FROM catalog.schema.table WHERE predicate
SELECT MIN(column) as metric_value FROM catalog.schema.table WHERE predicate
SELECT MAX(column) as metric_value FROM catalog.schema.table WHERE predicate
```

## Security and Privacy

### Data Protection
- ✅ **No raw data exposure**: Only aggregated values returned
- ✅ **Predicate-based filtering**: Users build conditions without seeing data
- ✅ **Catalog/schema allowlist**: Access control preserved
- ✅ **Audit trail**: All aggregation queries logged with request IDs

### Compliance
- Suitable for environments with strict data privacy requirements
- Safe for external agents and untrusted consumers
- Enables lead generation without data exposure

## Troubleshooting

### Migration Issues

**Q: I used to use `run_query` to get data, what now?**
A: Use `approx_count` to estimate audience size, then export conditions for your internal DS team to analyze actual data via Jupyter notebooks on Databricks.

**Q: How do I get detailed data breakdowns?**
A: Use `aggregate_metric` with GROUP BY... wait, we don't support GROUP BY yet. This is a future enhancement for multi-dimensional analysis.

**Q: Can I still execute Python code?**
A: No, job execution has been removed. Use the exported conditions to generate Jupyter notebooks that your internal DS team can run.

**Q: Why remove the data retrieval tools?**
A: To ensure privacy and compliance. The MCP server is now suitable for external-facing lead generation tools where raw data exposure is not acceptable.

## Future Enhancements

Potential additions to the lead generation MCP:

1. **GROUP BY Support**: Multi-dimensional audience segmentation
2. **Condition Export**: Generate Jupyter notebook parameters from predicates
3. **Percentile Metrics**: PERCENTILE_CONT for distribution analysis
4. **Time-Series Aggregation**: GROUP BY time windows for trend analysis
5. **User Attributes**: Row-level filtering based on user permissions

## Support and Questions

For questions about the migration:
- Review the updated [README.md](./README.md)
- Check the new [REQUIREMENTS.md](./REQUIREMENTS.md)
- See [DATABRICKS_RESOURCES_SUMMARY.md](./DATABRICKS_RESOURCES_SUMMARY.md) for available tables

