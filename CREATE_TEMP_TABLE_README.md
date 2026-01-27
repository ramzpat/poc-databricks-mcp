# create_temp_table Documentation Index

This directory contains comprehensive documentation for the new `create_temp_table` tool.

## 📚 Documentation Files

### Quick Start
- **[CREATE_TEMP_TABLE_QUICK_REF.md](CREATE_TEMP_TABLE_QUICK_REF.md)** - Quick reference card with examples, common errors, and troubleshooting tips. Start here for a quick overview.

### Detailed Guide
- **[CREATE_TEMP_TABLE_GUIDE.md](CREATE_TEMP_TABLE_GUIDE.md)** - Comprehensive usage guide covering all aspects of the tool including detailed examples, best practices, security considerations, and troubleshooting.

### Examples
- **[examples_create_temp_table.py](examples_create_temp_table.py)** - 6 practical, real-world examples demonstrating:
  1. High-value customer summary
  2. Qualified lead segments (multi-table JOINs)
  3. Campaign ROI analysis (cross-catalog)
  4. Technology sector lead scoring
  5. Regional market analysis
  6. ABM priority accounts (with CTEs)

### Implementation Details
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Complete implementation overview including what was built, how it works, and quality assurance details.
- **[IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md)** - Detailed checklist of all implementation tasks completed.

### Validation
- **[validate_create_temp_table.py](validate_create_temp_table.py)** - Comprehensive validation suite to verify the implementation.
- **[validate_implementation.py](validate_implementation.py)** - Quick syntax and import validation.

## 🚀 Getting Started

### 1. Update Configuration
First, update your `config.yml` to include CREATE in allowed statements:

```yaml
limits:
  allow_statement_types:
    - SELECT
    - CREATE  # Required for create_temp_table tool
```

### 2. Learn the Basics
Read the [Quick Reference](CREATE_TEMP_TABLE_QUICK_REF.md) to understand:
- Tool signature and parameters
- What queries are allowed
- Common error messages
- Simple examples

### 3. Try Examples
Run or review [examples_create_temp_table.py](examples_create_temp_table.py) to see practical use cases.

### 4. Deep Dive
Read the [Complete Guide](CREATE_TEMP_TABLE_GUIDE.md) for:
- Detailed parameter documentation
- Privacy-first design principles
- Advanced examples
- Best practices
- Troubleshooting

### 5. Validate
Run validation to ensure everything is set up correctly:
```bash
python validate_create_temp_table.py
```

## 📖 How to Use the Tool

### Basic Usage
```python
result = create_temp_table(
    temp_table_name="my_summary",
    sql_query="""
        SELECT 
            category,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM `main`.`default`.`data`
        GROUP BY category
    """
)

print(f"Created table with {result['row_count']} rows")
```

### Multi-Table JOIN
```python
result = create_temp_table(
    temp_table_name="customer_orders",
    sql_query="""
        SELECT 
            c.id,
            c.name,
            COUNT(o.id) as order_count,
            SUM(o.amount) as total_spent
        FROM `main`.`default`.`customers` c
        LEFT JOIN `main`.`sales`.`orders` o ON c.id = o.customer_id
        GROUP BY c.id, c.name
    """
)
```

### Cross-Catalog Analysis
```python
result = create_temp_table(
    temp_table_name="marketing_results",
    sql_query="""
        SELECT 
            m.campaign_id,
            COUNT(DISTINCT a.user_id) as reach
        FROM `main`.`marketing`.`campaigns` m
        JOIN `analytics`.`reporting`.`activity` a 
            ON m.id = a.campaign_id
        GROUP BY m.campaign_id
    """
)
```

## 🛡️ Security & Privacy

The tool maintains strict security guardrails:

✅ **SELECT Only** - Only SELECT queries allowed, no data modification  
✅ **Catalog Validation** - All referenced catalogs must be in allowlist  
✅ **Schema Validation** - All referenced schemas must be in allowlist  
✅ **Pattern Blocking** - Dangerous patterns like INTO OUTFILE are blocked  
✅ **Session Scoped** - Temporary views are automatically cleaned up  
✅ **Identifier Validation** - No SQL injection via table names  

## 🔍 Common Use Cases

1. **Lead Generation** - Combine companies, contacts, and transactions to identify qualified leads
2. **Customer Segmentation** - Aggregate customer data across multiple dimensions
3. **Campaign Analysis** - Analyze marketing campaign effectiveness across data sources
4. **Market Research** - Create regional or industry-specific aggregated views
5. **Lead Scoring** - Calculate multi-factor lead scores based on various criteria
6. **Account-Based Marketing** - Build priority account lists for ABM campaigns

## ❓ FAQ

**Q: Can I use INSERT, UPDATE, or DELETE statements?**  
A: No, only SELECT statements are allowed for privacy-first design.

**Q: How long does the temporary view exist?**  
A: Temporary views are session-scoped and automatically cleaned up when the session ends.

**Q: Can I reference tables from catalogs not in my allowlist?**  
A: No, all catalog and schema references must be in your configured allowlist.

**Q: What happens if my query fails?**  
A: A QueryError is raised with details, and the failure is logged with a request ID.

**Q: Can I query the temporary view after creating it?**  
A: Yes, within the same session. The view persists until the session ends.

**Q: Are temporary views persistent across sessions?**  
A: No, temporary views are session-scoped only.

## 📊 Documentation Overview

| Document | Purpose | Audience |
|----------|---------|----------|
| Quick Ref | Fast lookup, common patterns | All users |
| Guide | Comprehensive documentation | All users |
| Examples | Practical code samples | Developers |
| Implementation Summary | Technical details | Developers/DevOps |
| Implementation Checklist | Verification | Developers/QA |
| Validation Scripts | Testing | Developers/QA |

## 🔗 Related Documentation

- [Main README](README.md) - Project overview and setup
- [Configuration Guide](config.example.yml) - Configuration reference
- [Test Suite](tests/test_create_temp_table.py) - Test examples

## 💡 Tips

1. **Start Simple** - Test with simple queries before complex JOINs
2. **Use Request IDs** - Include request_id for better tracking
3. **Handle Errors** - Always wrap in try/except blocks
4. **Aggregate Data** - Focus on GROUP BY, not raw records
5. **Filter Early** - Use WHERE clauses to reduce data volume
6. **Monitor Resources** - Watch session resource usage for large results

## 🆘 Getting Help

1. Check the [Quick Reference](CREATE_TEMP_TABLE_QUICK_REF.md) troubleshooting section
2. Review [examples](examples_create_temp_table.py) for similar use cases
3. Read the [Complete Guide](CREATE_TEMP_TABLE_GUIDE.md) troubleshooting section
4. Check logs for detailed error messages (includes request IDs)
5. Verify your `config.yml` includes CREATE in allow_statement_types
6. Run validation scripts to check setup

## ✅ Quick Validation

Run this to verify everything is set up correctly:

```bash
# Validate implementation
python validate_create_temp_table.py

# Run full test suite
pytest tests/test_create_temp_table.py -v
```

## 📝 Feedback

If you find issues or have suggestions:
1. Check existing documentation first
2. Review implementation details in IMPLEMENTATION_SUMMARY.md
3. File an issue with:
   - Error message (if any)
   - Query you attempted
   - Configuration (without secrets)
   - Request ID from logs

---

**Ready to get started?** Read the [Quick Reference](CREATE_TEMP_TABLE_QUICK_REF.md) or dive into the [Complete Guide](CREATE_TEMP_TABLE_GUIDE.md)!
