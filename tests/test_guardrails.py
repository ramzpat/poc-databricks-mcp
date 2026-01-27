import pytest

from databricks_mcp.guardrails import clamp_limit, detect_statement_type, sanitize_identifier, validate_temp_table_query
from databricks_mcp.errors import GuardrailError
from databricks_mcp.config import ScopeConfig


def test_detect_statement_type() -> None:
    assert detect_statement_type("  select * from table") == "SELECT"
    with pytest.raises(GuardrailError):
        detect_statement_type("   ")


def test_sanitize_identifier() -> None:
    assert sanitize_identifier("valid_name", "table") == "valid_name"
    with pytest.raises(GuardrailError):
        sanitize_identifier("not-valid*", "table")


def test_clamp_limit() -> None:
    assert clamp_limit(5, 10) == 5
    assert clamp_limit(15, 10) == 10
    assert clamp_limit(None, 10) == 10
    assert clamp_limit(5, -1) == 5


def test_validate_temp_table_query_valid() -> None:
    """Test that valid SELECT queries pass validation."""
    scopes = ScopeConfig(catalogs={
        "main": ["default", "sales"],
        "analytics": ["reporting"]
    })
    
    # Simple SELECT
    validate_temp_table_query(
        "SELECT * FROM `main`.`default`.`users`",
        scopes
    )
    
    # SELECT with JOIN
    validate_temp_table_query(
        """
        SELECT c.id, c.name, COUNT(o.id) as order_count
        FROM `main`.`default`.`customers` c
        JOIN `main`.`sales`.`orders` o ON c.id = o.customer_id
        GROUP BY c.id, c.name
        """,
        scopes
    )
    
    # SELECT with multiple catalogs
    validate_temp_table_query(
        """
        SELECT a.metric, b.value
        FROM `main`.`default`.`table_a` a
        JOIN `analytics`.`reporting`.`table_b` b ON a.id = b.id
        """,
        scopes
    )
    
    # SELECT without backticks
    validate_temp_table_query(
        "SELECT * FROM main.default.users",
        scopes
    )


def test_validate_temp_table_query_non_select() -> None:
    """Test that non-SELECT statements are rejected."""
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    
    with pytest.raises(GuardrailError, match="must be a SELECT statement"):
        validate_temp_table_query(
            "INSERT INTO main.default.users VALUES (1, 'test')",
            scopes
        )
    
    with pytest.raises(GuardrailError, match="must be a SELECT statement"):
        validate_temp_table_query(
            "UPDATE main.default.users SET name = 'test'",
            scopes
        )
    
    with pytest.raises(GuardrailError, match="must be a SELECT statement"):
        validate_temp_table_query(
            "DELETE FROM main.default.users WHERE id = 1",
            scopes
        )
    
    with pytest.raises(GuardrailError, match="must be a SELECT statement"):
        validate_temp_table_query(
            "DROP TABLE main.default.users",
            scopes
        )


def test_validate_temp_table_query_forbidden_patterns() -> None:
    """Test that queries with forbidden patterns are rejected."""
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    
    # INTO OUTFILE
    with pytest.raises(GuardrailError, match="forbidden pattern"):
        validate_temp_table_query(
            "SELECT * FROM main.default.users INTO OUTFILE '/tmp/data.txt'",
            scopes
        )
    
    # EXEC
    with pytest.raises(GuardrailError, match="forbidden pattern"):
        validate_temp_table_query(
            "SELECT * FROM main.default.users; EXEC sp_something",
            scopes
        )
    
    # CALL
    with pytest.raises(GuardrailError, match="forbidden pattern"):
        validate_temp_table_query(
            "SELECT * FROM main.default.users; CALL dangerous_proc()",
            scopes
        )


def test_validate_temp_table_query_disallowed_catalog() -> None:
    """Test that queries referencing disallowed catalogs are rejected."""
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            "SELECT * FROM `forbidden_catalog`.`default`.`users`",
            scopes
        )
    
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            "SELECT * FROM forbidden_catalog.default.users",
            scopes
        )


def test_validate_temp_table_query_disallowed_schema() -> None:
    """Test that queries referencing disallowed schemas are rejected."""
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            "SELECT * FROM `main`.`forbidden_schema`.`users`",
            scopes
        )
    
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            "SELECT * FROM main.forbidden_schema.users",
            scopes
        )


def test_validate_temp_table_query_mixed_allowed_disallowed() -> None:
    """Test queries that mix allowed and disallowed catalogs/schemas."""
    scopes = ScopeConfig(catalogs={
        "main": ["default"],
        "analytics": ["reporting"]
    })
    
    # One allowed, one disallowed catalog
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            """
            SELECT *
            FROM `main`.`default`.`users` u
            JOIN `forbidden`.`default`.`orders` o ON u.id = o.user_id
            """,
            scopes
        )
    
    # Allowed catalog, one allowed and one disallowed schema
    with pytest.raises(GuardrailError, match="not in allowlist"):
        validate_temp_table_query(
            """
            SELECT *
            FROM `main`.`default`.`users` u
            JOIN `main`.`forbidden_schema`.`orders` o ON u.id = o.user_id
            """,
            scopes
        )
