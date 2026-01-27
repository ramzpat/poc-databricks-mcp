import pytest
from unittest.mock import MagicMock, Mock, patch
from databricks.sql.exc import Error as DatabricksError

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
    """Test successful creation of global temporary table."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    # Mock databricks.sql.connect and cursor behavior
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = [(100,)]  # Row count result
    
    mock_connection = MagicMock()
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="qualified_leads",
            source_query="SELECT * FROM main.default.customers WHERE score > 80",
            request_id="test-request-1",
        )
    
    assert result["temp_table_name"] == "global_temp.qualified_leads"
    assert result["row_count"] == 100
    assert result["status"] == "created"
    
    # Verify CREATE GLOBAL TEMPORARY VIEW was called
    assert mock_cursor.execute.call_count == 2
    create_call = mock_cursor.execute.call_args_list[0]
    assert "CREATE OR REPLACE GLOBAL TEMPORARY VIEW" in create_call[0][0]
    assert "global_temp.`qualified_leads`" in create_call[0][0]


def test_create_temp_table_with_join(mocker):
    """Test creating global temporary table with JOIN from multiple sources."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = [(50,)]
    
    mock_connection = MagicMock()
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
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
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="high_value_leads",
            source_query=source_query,
            request_id="test-request-2",
        )
    
    assert result["temp_table_name"] == "global_temp.high_value_leads"
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


def test_create_temp_table_with_semicolon():
    """Test that queries with semicolons (statement terminators) are rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    with pytest.raises(GuardrailError) as exc_info:
        client.create_temp_table(
            temp_table_name="test_table",
            source_query="SELECT * FROM main.default.customers; DROP TABLE important",
        )
    
    assert "cannot contain statement terminators" in str(exc_info.value)


def test_create_temp_table_creation_failure(mocker):
    """Test handling of database errors during table creation."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    # Mock databricks.sql.connect to raise an error
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = DatabricksError("Table creation failed")
    
    mock_connection = MagicMock()
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        with pytest.raises(QueryError) as exc_info:
            client.create_temp_table(
                temp_table_name="test_table",
                source_query="SELECT * FROM main.default.customers",
            )
    
    assert "Failed to create temporary table" in str(exc_info.value)


def test_create_temp_table_with_aggregation(mocker):
    """Test creating global temporary table with aggregated data."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = [(10,)]
    
    mock_connection = MagicMock()
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
    source_query = """
    SELECT 
        region,
        COUNT(*) as customer_count,
        SUM(total_purchases) as total_revenue
    FROM main.default.customers
    GROUP BY region
    HAVING SUM(total_purchases) > 10000
    """
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="regional_summary",
            source_query=source_query,
            request_id="test-request-3",
        )
    
    assert result["temp_table_name"] == "global_temp.regional_summary"
    assert result["row_count"] == 10
    assert result["status"] == "created"
