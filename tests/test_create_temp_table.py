import pytest
from unittest.mock import MagicMock, Mock

from databricks_mcp.client import DatabricksSQLClient
from databricks_mcp.config import AppConfig, LimitsConfig, OAuthConfig, ScopeConfig, WarehouseConfig, ObservabilityConfig
from databricks_mcp.errors import GuardrailError, QueryError


def create_test_config() -> AppConfig:
    """Create a test configuration with allowlisted catalogs and schemas."""
    return AppConfig(
        warehouse=WarehouseConfig(
            host="https://test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            warehouse_id="test-warehouse",
        ),
        oauth=OAuthConfig(
            client_id="test-client",
            client_secret="test-secret",
            token_url="https://test.databricks.com/oidc/v1/token",
        ),
        scopes=ScopeConfig(
            catalogs={
                "main": ["default", "analytics"],
                "prod": ["sales"],
            }
        ),
        limits=LimitsConfig(
            max_rows=1000,
            sample_max_rows=100,
            query_timeout_seconds=30,
            max_concurrent_queries=5,
            allow_statement_types=["SELECT"],
        ),
        observability=ObservabilityConfig(
            log_level="info",
            propagate_request_ids=True,
        ),
    )


def test_create_temp_table_success(mocker):
    """Test successful creation of temporary table."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    # Mock the _execute method to simulate successful CREATE and COUNT queries
    execute_results = [
        ([], False),  # CREATE TEMPORARY VIEW returns empty result
        ([{"row_count": 100}], False),  # COUNT query returns row count
    ]
    mocker.patch.object(client, '_execute', side_effect=execute_results)
    
    result = client.create_temp_table(
        temp_table_name="qualified_leads",
        source_query="SELECT * FROM main.default.customers WHERE score > 80",
        request_id="test-request-1",
    )
    
    assert result["temp_table_name"] == "qualified_leads"
    assert result["row_count"] == 100
    assert result["status"] == "created"


def test_create_temp_table_with_join(mocker):
    """Test creating temporary table with JOIN from multiple sources."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    execute_results = [
        ([], False),
        ([{"row_count": 50}], False),
    ]
    mocker.patch.object(client, '_execute', side_effect=execute_results)
    
    source_query = """
    SELECT 
        t1.customer_id,
        t1.total_purchases,
        t2.engagement_score
    FROM main.default.purchases t1
    JOIN main.analytics.engagement t2 
        ON t1.customer_id = t2.customer_id
    WHERE t1.total_purchases > 1000 
        AND t2.engagement_score > 0.7
    """
    
    result = client.create_temp_table(
        temp_table_name="high_value_leads",
        source_query=source_query,
        request_id="test-request-2",
    )
    
    assert result["temp_table_name"] == "high_value_leads"
    assert result["row_count"] == 50
    assert result["status"] == "created"


def test_create_temp_table_invalid_name():
    """Test that invalid table names are rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    with pytest.raises(GuardrailError):
        client.create_temp_table(
            temp_table_name="invalid-name-with-dashes",
            source_query="SELECT * FROM main.default.customers",
        )


def test_create_temp_table_non_select_query():
    """Test that non-SELECT queries are rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    with pytest.raises(GuardrailError) as exc_info:
        client.create_temp_table(
            temp_table_name="bad_table",
            source_query="INSERT INTO main.default.customers VALUES (1, 'test')",
        )
    
    assert "must be a SELECT statement" in str(exc_info.value)


def test_create_temp_table_empty_query():
    """Test that empty query is rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    with pytest.raises(GuardrailError):
        client.create_temp_table(
            temp_table_name="test_table",
            source_query="   ",
        )


def test_create_temp_table_creation_failure(mocker):
    """Test handling of database errors during table creation."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    # Mock _execute to raise QueryError on the CREATE statement
    mocker.patch.object(
        client, 
        '_execute', 
        side_effect=QueryError("Table creation failed: table already exists")
    )
    
    with pytest.raises(QueryError) as exc_info:
        client.create_temp_table(
            temp_table_name="test_table",
            source_query="SELECT * FROM main.default.customers",
        )
    
    assert "Failed to create temporary table" in str(exc_info.value)


def test_create_temp_table_with_aggregation(mocker):
    """Test creating temporary table with aggregated data."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    execute_results = [
        ([], False),
        ([{"row_count": 10}], False),
    ]
    mocker.patch.object(client, '_execute', side_effect=execute_results)
    
    source_query = """
    SELECT 
        region,
        COUNT(*) as customer_count,
        SUM(total_purchases) as total_revenue
    FROM main.default.customers
    GROUP BY region
    HAVING SUM(total_purchases) > 10000
    """
    
    result = client.create_temp_table(
        temp_table_name="regional_summary",
        source_query=source_query,
        request_id="test-request-3",
    )
    
    assert result["temp_table_name"] == "regional_summary"
    assert result["row_count"] == 10
    assert result["status"] == "created"
