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
    """Test successful creation of global temporary table with structured parameters."""
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
    
    source_tables = [
        {"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}
    ]
    columns = [
        {"table_alias": "c", "column": "customer_id"},
        {"table_alias": "c", "column": "score"}
    ]
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="qualified_leads",
            source_tables=source_tables,
            columns=columns,
            where_conditions="c.score > 80",
            request_id="test-request-1",
        )
    
    assert result["temp_table_name"] == "global_temp.qualified_leads"
    assert result["row_count"] == 100
    assert result["status"] == "created"
    assert "session-scoped" in result["note"]
    
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
    
    source_tables = [
        {"catalog": "main", "schema": "default", "table": "purchases", "alias": "p"},
        {"catalog": "main", "schema": "analytics", "table": "engagement", "alias": "e"}
    ]
    columns = [
        {"table_alias": "p", "column": "customer_id"},
        {"table_alias": "p", "column": "total_purchases", "alias": "total_spent"},
        {"table_alias": "e", "column": "engagement_score"}
    ]
    join_conditions = [
        {
            "type": "INNER",
            "left_table": "p",
            "left_column": "customer_id",
            "right_table": "e",
            "right_column": "customer_id"
        }
    ]
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="high_value_leads",
            source_tables=source_tables,
            columns=columns,
            join_conditions=join_conditions,
            where_conditions="p.total_purchases > 1000 AND e.engagement_score > 0.7",
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
    
    source_tables = [{"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}]
    columns = [{"table_alias": "c", "column": "id"}]
    
    with pytest.raises(GuardrailError):
        client.create_temp_table(
            temp_table_name="invalid-name-with-dashes",
            source_tables=source_tables,
            columns=columns,
        )


def test_create_temp_table_empty_tables():
    """Test that empty source tables list is rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    with pytest.raises(GuardrailError) as exc_info:
        client.create_temp_table(
            temp_table_name="test_table",
            source_tables=[],
            columns=[{"table_alias": "t", "column": "id"}],
        )
    
    assert "At least one source table is required" in str(exc_info.value)


def test_create_temp_table_empty_columns():
    """Test that empty columns list is rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    source_tables = [{"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}]
    
    with pytest.raises(GuardrailError) as exc_info:
        client.create_temp_table(
            temp_table_name="test_table",
            source_tables=source_tables,
            columns=[],
        )
    
    assert "At least one column is required" in str(exc_info.value)


def test_create_temp_table_with_semicolon_in_where():
    """Test that WHERE conditions with semicolons are rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    source_tables = [{"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}]
    columns = [{"table_alias": "c", "column": "id"}]
    
    with pytest.raises(GuardrailError) as exc_info:
        client.create_temp_table(
            temp_table_name="test_table",
            source_tables=source_tables,
            columns=columns,
            where_conditions="c.id > 1; DROP TABLE important",
        )
    
    assert "cannot contain semicolons" in str(exc_info.value)


def test_create_temp_table_non_allowlisted_catalog():
    """Test that non-allowlisted catalogs are rejected."""
    config = create_test_config()
    token_provider = MagicMock()
    client = DatabricksSQLClient(config, token_provider)
    
    source_tables = [{"catalog": "forbidden", "schema": "default", "table": "customers", "alias": "c"}]
    columns = [{"table_alias": "c", "column": "id"}]
    
    from databricks_mcp.errors import ScopeError
    with pytest.raises(ScopeError):
        client.create_temp_table(
            temp_table_name="test_table",
            source_tables=source_tables,
            columns=columns,
        )


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
    
    source_tables = [{"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}]
    columns = [{"table_alias": "c", "column": "id"}]
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        with pytest.raises(QueryError) as exc_info:
            client.create_temp_table(
                temp_table_name="test_table",
                source_tables=source_tables,
                columns=columns,
            )
    
    assert "Failed to create temporary table" in str(exc_info.value)


def test_create_temp_table_with_column_aliases(mocker):
    """Test creating temporary table with column aliases."""
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
    
    source_tables = [{"catalog": "main", "schema": "default", "table": "customers", "alias": "c"}]
    columns = [
        {"table_alias": "c", "column": "customer_id", "alias": "id"},
        {"table_alias": "c", "column": "total_purchases", "alias": "revenue"}
    ]
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="renamed_cols",
            source_tables=source_tables,
            columns=columns,
            request_id="test-request-3",
        )
    
    assert result["temp_table_name"] == "global_temp.renamed_cols"
    assert result["row_count"] == 10
    assert result["status"] == "created"


def test_create_temp_table_with_left_join(mocker):
    """Test creating temporary table with LEFT JOIN."""
    config = create_test_config()
    token_provider = MagicMock()
    token_provider.get_token.return_value = "test-token"
    
    client = DatabricksSQLClient(config, token_provider)
    
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock()
    mock_cursor.fetchall.return_value = [(75,)]
    
    mock_connection = MagicMock()
    mock_connection.__enter__ = MagicMock(return_value=mock_connection)
    mock_connection.__exit__ = MagicMock(return_value=False)
    mock_connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_connection.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
    source_tables = [
        {"catalog": "main", "schema": "default", "table": "customers", "alias": "c"},
        {"catalog": "main", "schema": "analytics", "table": "preferences", "alias": "p"}
    ]
    columns = [
        {"table_alias": "c", "column": "customer_id"},
        {"table_alias": "p", "column": "preference_score"}
    ]
    join_conditions = [
        {
            "type": "LEFT",
            "left_table": "c",
            "left_column": "customer_id",
            "right_table": "p",
            "right_column": "customer_id"
        }
    ]
    
    with patch('databricks.sql.connect', return_value=mock_connection):
        result = client.create_temp_table(
            temp_table_name="customers_with_prefs",
            source_tables=source_tables,
            columns=columns,
            join_conditions=join_conditions,
            request_id="test-request-4",
        )
    
    assert result["temp_table_name"] == "global_temp.customers_with_prefs"
    assert result["row_count"] == 75
    assert result["status"] == "created"
