---
name: sql-optimizer
description: Optimize SQL queries for Databricks analytics with automatic data profiling, partition analysis, and performance optimization. Use when users request data analysis, SQL queries, or analytics tasks that involve large tables. The skill ensures queries are optimized by checking table metadata, partition columns, data size, and applying best practices before executing queries. Essential for interactive analytics requiring good performance on large datasets.
---

# SQL Optimizer for Interactive Analytics

## Overview

Optimizes SQL queries for Databricks by understanding data characteristics before query execution. Ensures partition pruning, appropriate sampling, and query efficiency for interactive analytics on large datasets.

## Core Workflow

Follow this workflow for ALL analytics requests involving SQL queries:

### 1. Understand the Data First

Before writing any SQL query, ALWAYS profile the target tables:

**Check table metadata:**
```
Use: mcp_data-analytic_table_metadata
Purpose: Get column names, types, row count, size, and schema
```

**Analyze partitioning:**
```
Use: mcp_data-analytic_partition_info
Purpose: Identify partition columns and their distribution
Critical: Queries MUST filter on partition columns to avoid full scans
```

**Sample the data:**
```
Use: mcp_data-analytic_sample_data
Purpose: Preview actual data and understand patterns
Limit: Start with 100-1000 rows for quick exploration
```

### 2. Design Optimized Query

Based on data profiling, apply these principles:

**Partition Pruning (MANDATORY for large tables >10GB):**
- Always include WHERE filters on partition columns
- Use date ranges for time-partitioned tables
- Filter on all partition levels for multi-column partitioning

**Column Selection:**
- Select only required columns (avoid SELECT *)
- Wide tables benefit most from column pruning

**Data Volume Control:**
- Apply WHERE filters before aggregations
- Use LIMIT for exploratory queries
- Consider TABLESAMPLE for very large tables (>1TB)

**Aggregation Strategy:**
- Use APPROX_COUNT_DISTINCT for approximate counts on huge datasets
- Pre-aggregate before joins when possible
- Filter before GROUP BY

### 3. Preview Before Full Execution

For large queries, always preview first:

**Preview query:**
```
Use: mcp_data-analytic_preview_query
Purpose: Test query on limited rows (~1000) to validate logic
Benefit: Fast feedback without full table scan
```

**Run full query:**
```
Use: mcp_data-analytic_run_query
Purpose: Execute optimized query after preview validation
Note: Only after confirming query logic is correct
```

## Decision Tree

```
Analytics Request Received
    ↓
Does user specify table(s)?
    ├─ No → Ask for clarification OR use list_schemas/list_tables to discover
    └─ Yes → Continue
        ↓
    Get table metadata (size, columns, row count)
        ↓
    Table size > 10GB?
    ├─ Yes → Get partition info (REQUIRED)
    │         ↓
    │     Has partition columns?
    │     ├─ Yes → MUST include partition filters in query
    │     └─ No → Consider sampling or time-based filters
    │
    └─ No → Optional partition check
        ↓
    Sample data (100-1000 rows)
        ↓
    Understand data patterns and structure
        ↓
    Design query with optimizations:
    - Partition filters (if applicable)
    - Column selection (not SELECT *)
    - Appropriate WHERE clauses
    - Pre-aggregation if needed
        ↓
    Query returns > 10K rows expected?
    ├─ Yes → Use preview_query first
    └─ No → Can use run_query directly
        ↓
    Execute query
        ↓
    Present results with insights
```

## Size Thresholds and Strategies

### Small Tables (<1GB)
- Direct queries acceptable
- Partition checks optional
- Full scans tolerable

### Medium Tables (1GB - 10GB)
- Check partitions recommended
- Use partition filters when available
- Select specific columns

### Large Tables (10GB - 100GB)
- Partition filters MANDATORY
- Always check partition_info first
- Use preview_query for validation
- Avoid SELECT *

### Very Large Tables (>100GB)
- Partition filters ABSOLUTELY REQUIRED
- Consider TABLESAMPLE for exploration
- Use APPROX functions for counts
- Start with preview_query
- May need multiple sampling passes

## Optimization Patterns

For detailed optimization patterns including:
- Partition pruning strategies
- Join optimization
- Aggregation techniques
- Common anti-patterns

See: [references/optimization_patterns.md](references/optimization_patterns.md)

## Example Workflows

### Example 1: User asks "How many orders were placed last month?"

**Step 1: Profile the data**
```
table_metadata(catalog="main", schema="sales", table="orders")
# Returns: 50GB, 500M rows, columns: order_id, customer_id, order_date, amount, status

partition_info(catalog="main", schema="sales", table="orders")
# Returns: Partitioned by order_date (daily)
```

**Step 2: Design query with partition filter**
```sql
SELECT COUNT(*) as total_orders
FROM main.sales.orders
WHERE order_date >= '2024-12-01'
  AND order_date < '2025-01-01'
```

**Step 3: Execute**
Since it's a simple aggregation with partition pruning, use run_query directly.

### Example 2: User asks "Show me sample customer data for analysis"

**Step 1: Profile**
```
table_metadata(catalog="main", schema="crm", table="customers")
# Returns: 200GB, columns: id, name, email, created_at, attributes (JSON)

partition_info(...)
# Returns: Partitioned by created_at (monthly)
```

**Step 2: Sample with partition filter**
```sql
SELECT id, name, email, created_at
FROM main.crm.customers
WHERE created_at >= '2024-01-01'  -- Recent data only
LIMIT 1000
```

**Step 3: Execute with preview_query** for safety.

### Example 3: User asks "Analyze user behavior patterns"

**Step 1: Discover and profile tables**
```
list_tables(catalog="main", schema="events")
# Find events table

table_metadata(catalog="main", schema="events", table="user_events")
# Returns: 2TB, 10B rows, columns: event_id, user_id, event_type, timestamp, properties

partition_info(...)
# Returns: Partitioned by timestamp (hourly)
```

**Step 2: Sample for exploration**
```
sample_data(catalog="main", schema="events", table="user_events", 
            predicate="timestamp >= '2024-12-01'", limit=1000)
```

**Step 3: Design optimized analysis**
```sql
SELECT 
  event_type,
  COUNT(*) as event_count,
  APPROX_COUNT_DISTINCT(user_id) as unique_users
FROM main.events.user_events
WHERE timestamp >= '2024-12-01'  -- Partition filter
  AND timestamp < '2025-01-01'
GROUP BY event_type
ORDER BY event_count DESC
```

**Step 4: Preview first, then run full query**

## Performance Checklist

Before executing any query, verify:

- [ ] Table metadata retrieved and understood
- [ ] For tables >10GB: Partition info checked
- [ ] Query includes partition column filters (if table is partitioned)
- [ ] Only necessary columns selected
- [ ] WHERE filters applied before aggregations
- [ ] For exploratory queries: Using LIMIT or TABLESAMPLE
- [ ] For large result sets: Used preview_query first
- [ ] Query follows optimization patterns from references/

## Common Mistakes to Avoid

1. **Skipping data profiling**: Never write queries without understanding the data
2. **Ignoring partitions**: Full scans on large partitioned tables cause poor performance
3. **SELECT * on wide tables**: Reading unnecessary columns wastes I/O
4. **No preview for large queries**: Test logic before full execution
5. **Missing WHERE clauses**: Always filter data appropriately
6. **Exact counts on huge tables**: Use APPROX_COUNT_DISTINCT when approximate is acceptable

## Resources

This skill includes:

- **references/optimization_patterns.md**: Detailed SQL optimization patterns, join strategies, aggregation techniques, and anti-patterns. Load when designing complex queries or troubleshooting performance issues.

