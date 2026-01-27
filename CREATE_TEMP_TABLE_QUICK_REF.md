# create_temp_table Quick Reference Card

## Tool Signature
```python
create_temp_table(
    temp_table_name: str,      # Required: Valid SQL identifier
    sql_query: str,            # Required: SELECT query
    request_id: str | None     # Optional: Request tracking ID
) -> dict[str, Any]
```

## Return Value
```json
{
  "temp_table_name": "your_table_name",
  "row_count": 150,
  "status": "created",
  "scope": "session",
  "note": "This temporary view is session-scoped and will be automatically cleaned up when the session ends"
}
```

## Requirements

### Configuration (config.yml)
```yaml
limits:
  allow_statement_types:
    - SELECT
    - CREATE  # Required!
```

### Query Constraints
- ✅ Must be SELECT statement
- ✅ All catalogs/schemas must be in allowlist
- ✅ Can include JOINs, WHERE, GROUP BY, aggregations
- ❌ No INSERT, UPDATE, DELETE, DROP
- ❌ No INTO OUTFILE, EXEC, CALL
- ❌ No disallowed catalogs/schemas

### Table Name Constraints
- ✅ Alphanumeric and underscores only
- ✅ Must start with letter or underscore
- ❌ No dashes, spaces, special characters

## Quick Examples

### Simple Aggregation
```python
create_temp_table(
    "customer_summary",
    "SELECT customer_id, COUNT(*) as orders FROM `main`.`sales`.`orders` GROUP BY customer_id"
)
```

### Multi-Table JOIN
```python
create_temp_table(
    "qualified_leads",
    """
    SELECT c.id, c.name, COUNT(ct.id) as contacts
    FROM `main`.`default`.`companies` c
    LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
    GROUP BY c.id, c.name
    """
)
```

### Cross-Catalog
```python
create_temp_table(
    "campaign_metrics",
    """
    SELECT m.campaign_id, COUNT(DISTINCT a.user_id) as reach
    FROM `main`.`marketing`.`campaigns` m
    JOIN `analytics`.`reporting`.`activity` a ON m.id = a.campaign_id
    GROUP BY m.campaign_id
    """
)
```

## Error Handling

```python
from databricks_mcp.errors import GuardrailError, QueryError

try:
    result = create_temp_table("my_temp", "SELECT ...")
    print(f"Created with {result['row_count']} rows")
except GuardrailError as e:
    print(f"Validation failed: {e}")
except QueryError as e:
    print(f"Execution failed: {e}")
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Invalid identifier for temp_table_name` | Special characters in name | Use only `a-zA-Z0-9_` |
| `must be a SELECT statement` | Non-SELECT query | Start with SELECT |
| `catalog 'X' which is not in allowlist` | Disallowed catalog | Use allowlisted catalogs |
| `schema 'Y' which is not in allowlist` | Disallowed schema | Check config.yml |
| `Query contains forbidden pattern` | Blocked SQL pattern | Remove INTO OUTFILE, EXEC, etc. |

## Best Practices

1. **Use descriptive names**: `qualified_leads_2024` not `temp1`
2. **Aggregate early**: Focus on GROUP BY, not raw records
3. **Filter in WHERE**: Reduce data volume before aggregation
4. **Add request IDs**: For tracking and debugging
5. **Handle errors**: Wrap in try/except
6. **Document queries**: Add comments to complex SQL

## Limitations

- ⚠️ Session-scoped only (not persistent)
- ⚠️ Cannot ALTER or DROP temp view after creation
- ⚠️ Row count query adds overhead for large results
- ⚠️ Subject to query timeout (default 60s)
- ⚠️ Respects max_concurrent_queries limit

## Performance Tips

- Filter early with WHERE clauses
- Use appropriate aggregations
- Avoid SELECT * when possible
- Monitor session resource usage
- Consider row count overhead on very large results

## Security Features

✅ SQL identifier validation (no injection)  
✅ SELECT-only enforcement  
✅ Catalog/schema allowlist validation  
✅ Forbidden pattern blocking  
✅ Session-scoped (no persistence)  
✅ Structured logging (no secrets)  

## Related Tools

- `list_catalogs`, `list_schemas`, `list_tables` - Discover data sources
- `table_metadata` - Understand table structure
- `approx_count` - Get quick row counts
- `aggregate_metric` - Single aggregation calculations
- `create_temp_table` - Multi-table aggregated views (this tool)

## Documentation

- Full Guide: `CREATE_TEMP_TABLE_GUIDE.md`
- Examples: `examples_create_temp_table.py`
- Tests: `tests/test_create_temp_table.py`
- Implementation: `IMPLEMENTATION_SUMMARY.md`

## Support

For issues or questions:
1. Check `CREATE_TEMP_TABLE_GUIDE.md` troubleshooting section
2. Review examples in `examples_create_temp_table.py`
3. Check logs for detailed error messages
4. Verify config.yml includes CREATE statement
5. Test with simple queries before complex ones
