# SQL Optimization Patterns for Databricks

## Partition Pruning Patterns

### Pattern 1: Date-based Partitions
When querying date-partitioned tables, always include date filters to avoid full scans:

```sql
-- ❌ BAD: Full table scan
SELECT COUNT(*) FROM sales_data WHERE amount > 1000;

-- ✅ GOOD: Partition pruning with date filter
SELECT COUNT(*) FROM sales_data 
WHERE date_col >= '2024-01-01' 
  AND date_col < '2024-02-01'
  AND amount > 1000;
```

### Pattern 2: Multi-column Partitioning
When tables have multiple partition columns, filter on all partition levels:

```sql
-- ❌ BAD: Only filters on one partition column
SELECT * FROM orders WHERE region = 'US';

-- ✅ GOOD: Filters on all partition columns
SELECT * FROM orders 
WHERE year = 2024 
  AND month = 1 
  AND region = 'US';
```

## Join Optimization

### Pattern 3: Broadcast Joins
For small dimension tables (<10GB), use broadcast hints:

```sql
-- ✅ GOOD: Broadcast small dimension table
SELECT /*+ BROADCAST(dim_users) */ 
  f.order_id, d.user_name
FROM fact_orders f
JOIN dim_users d ON f.user_id = d.user_id;
```

### Pattern 4: Pre-aggregate Before Joins
Reduce data volume before expensive joins:

```sql
-- ❌ BAD: Join then aggregate
SELECT customer_id, SUM(amount)
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
GROUP BY customer_id;

-- ✅ GOOD: Aggregate then join
WITH item_totals AS (
  SELECT order_id, SUM(amount) as total
  FROM order_items
  GROUP BY order_id
)
SELECT o.customer_id, SUM(t.total)
FROM orders o
JOIN item_totals t ON o.order_id = t.order_id
GROUP BY o.customer_id;
```

## Data Volume Management

### Pattern 5: Limit Early
Apply LIMIT and WHERE filters as early as possible:

```sql
-- ❌ BAD: Process all data then limit
SELECT * FROM (
  SELECT customer_id, order_date, amount
  FROM large_orders
  ORDER BY amount DESC
) LIMIT 100;

-- ✅ GOOD: Use subquery with ORDER BY LIMIT
SELECT customer_id, order_date, amount
FROM large_orders
ORDER BY amount DESC
LIMIT 100;
```

### Pattern 6: Sampling
For exploratory analysis on large tables, use sampling:

```sql
-- For quick data exploration
SELECT * FROM large_table TABLESAMPLE (1 PERCENT);

-- Or fixed row count
SELECT * FROM large_table LIMIT 10000;
```

## Aggregation Optimization

### Pattern 7: Use Approximate Functions
For large datasets, use approximate functions when exact counts aren't critical:

```sql
-- ❌ Exact count (expensive on huge tables)
SELECT COUNT(DISTINCT user_id) FROM events;

-- ✅ Approximate count (much faster)
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events;
```

### Pattern 8: Filter Before Aggregation
Apply WHERE filters before GROUP BY:

```sql
-- ✅ GOOD: Filter then aggregate
SELECT region, COUNT(*)
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region;
```

## Columnar Storage Optimization

### Pattern 9: Select Only Required Columns
Avoid SELECT * on wide tables:

```sql
-- ❌ BAD: Reads all columns
SELECT * FROM wide_table WHERE id = 123;

-- ✅ GOOD: Select only needed columns
SELECT id, name, created_at FROM wide_table WHERE id = 123;
```

## Common Anti-patterns to Avoid

1. **Cross Joins**: Avoid unintentional cartesian products
2. **UDFs in Filters**: User-defined functions prevent partition pruning
3. **OR Conditions on Partitions**: Use UNION instead for better partition pruning
4. **Nested Subqueries**: Flatten when possible or use CTEs for clarity
5. **Non-equi Joins**: Range joins can be expensive; consider bucketing

## Performance Indicators

Watch for these signs of inefficiency:
- **Spill to disk**: Data doesn't fit in memory
- **Skewed partitions**: One partition much larger than others
- **Long shuffles**: Data movement across nodes
- **High partition count**: Too many small files (<1MB)
