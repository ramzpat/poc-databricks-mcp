import pytest

from databricks_mcp.guardrails import clamp_limit, detect_statement_type, sanitize_identifier, ensure_catalog_allowed, ensure_schema_allowed
from databricks_mcp.errors import GuardrailError, ScopeError
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


def test_ensure_catalog_allowed_regular() -> None:
    """Test that regular catalogs must be in allowlist."""
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    # Should not raise for allowed catalog
    ensure_catalog_allowed("main", scopes)
    # Should raise for non-allowed catalog
    with pytest.raises(ScopeError):
        ensure_catalog_allowed("forbidden", scopes)

