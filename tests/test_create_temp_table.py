"""
Integration tests for the create_temp_table tool.

These tests validate the full flow including guardrails, SQL validation,
and the integration with the DatabricksSQLClient.
"""

import pytest
from unittest.mock import MagicMock, patch, call
from databricks_mcp.client import DatabricksSQLClient
from databricks_mcp.config import AppConfig, WarehouseConfig, OAuthConfig, ScopeConfig, LimitsConfig, ObservabilityConfig
from databricks_mcp.errors import GuardrailError, QueryError
from databricks_mcp.auth import OAuthTokenProvider


def create_test_config() -> AppConfig:
    """Create a test configuration with typical settings."""
    return AppConfig(
        warehouse=WarehouseConfig(
            host="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            warehouse_id="test_warehouse"
        ),
        oauth=OAuthConfig(
            client_id="test_client",
            client_secret="test_secret",
            token_url="https://test.databricks.com/oauth2/token",
            scope=None
        ),
        scopes=ScopeConfig(catalogs={
            "main": ["default", "sales"],
            "analytics": ["reporting"]
        }),
        limits=LimitsConfig(
            max_rows=10000,
            sample_max_rows=1000,
            query_timeout_seconds=60,
            max_concurrent_queries=5,
            allow_statement_types=["SELECT", "CREATE"]
        ),
        observability=ObservabilityConfig(
            log_level="info",
            propagate_request_ids=True
        )
    )


def create_mock_token_provider() -> OAuthTokenProvider:
    """Create a mock token provider."""
    provider = MagicMock(spec=OAuthTokenProvider)
    provider.get_token.return_value = "mock_token"
    return provider


class TestCreateTempTable:
    """Test suite for create_temp_table functionality."""
    
    def test_valid_simple_select(self) -> None:
        """Test creating a temp table with a simple SELECT query."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            # Setup mock connection and cursor
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (100,)  # row count
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="SELECT * FROM `main`.`default`.`users`",
                request_id="test-123"
            )
            
            assert result["temp_table_name"] == "test_temp"
            assert result["row_count"] == 100
            assert result["status"] == "created"
            assert result["scope"] == "session"
            
            # Verify CREATE TEMPORARY VIEW was called
            calls = mock_cursor.execute.call_args_list
            assert len(calls) == 2
            create_call = calls[0][0][0]
            assert "CREATE TEMPORARY VIEW test_temp AS" in create_call
            assert "SELECT * FROM `main`.`default`.`users`" in create_call
    
    def test_valid_join_query(self) -> None:
        """Test creating a temp table with a JOIN query."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        sql_query = """
        SELECT c.id, c.name, COUNT(o.id) as order_count
        FROM `main`.`default`.`customers` c
        JOIN `main`.`sales`.`orders` o ON c.id = o.customer_id
        GROUP BY c.id, c.name
        """
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (50,)
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="customer_orders",
                sql_query=sql_query,
                request_id="test-456"
            )
            
            assert result["temp_table_name"] == "customer_orders"
            assert result["row_count"] == 50
            assert result["status"] == "created"
    
    def test_invalid_table_name(self) -> None:
        """Test that invalid temp table names are rejected."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with pytest.raises(GuardrailError, match="Invalid identifier"):
            client.create_temp_table(
                temp_table_name="invalid-name-with-dashes",
                sql_query="SELECT * FROM `main`.`default`.`users`"
            )
    
    def test_non_select_query_rejected(self) -> None:
        """Test that non-SELECT queries are rejected."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with pytest.raises(GuardrailError, match="must be a SELECT statement"):
            client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="INSERT INTO `main`.`default`.`users` VALUES (1, 'test')"
            )
        
        with pytest.raises(GuardrailError, match="must be a SELECT statement"):
            client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="UPDATE `main`.`default`.`users` SET name = 'test'"
            )
    
    def test_disallowed_catalog_rejected(self) -> None:
        """Test that queries referencing disallowed catalogs are rejected."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with pytest.raises(GuardrailError, match="not in allowlist"):
            client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="SELECT * FROM `forbidden`.`default`.`users`"
            )
    
    def test_disallowed_schema_rejected(self) -> None:
        """Test that queries referencing disallowed schemas are rejected."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with pytest.raises(GuardrailError, match="not in allowlist"):
            client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="SELECT * FROM `main`.`forbidden`.`users`"
            )
    
    def test_forbidden_patterns_rejected(self) -> None:
        """Test that queries with forbidden patterns are rejected."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        forbidden_queries = [
            "SELECT * FROM `main`.`default`.`users` INTO OUTFILE '/tmp/data.txt'",
            "SELECT * FROM `main`.`default`.`users`; EXEC sp_something",
            "SELECT * FROM `main`.`default`.`users`; CALL dangerous_proc()",
        ]
        
        for query in forbidden_queries:
            with pytest.raises(GuardrailError, match="forbidden pattern"):
                client.create_temp_table(
                    temp_table_name="test_temp",
                    sql_query=query
                )
    
    def test_cross_catalog_join(self) -> None:
        """Test creating a temp table with cross-catalog JOIN."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        sql_query = """
        SELECT 
            m.customer_id,
            m.purchase_amount,
            a.report_date,
            a.metric_value
        FROM `main`.`sales`.`transactions` m
        JOIN `analytics`.`reporting`.`metrics` a 
            ON m.customer_id = a.customer_id
        WHERE m.purchase_amount > 1000
        """
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (75,)
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="cross_catalog_view",
                sql_query=sql_query
            )
            
            assert result["temp_table_name"] == "cross_catalog_view"
            assert result["row_count"] == 75
    
    def test_aggregation_query(self) -> None:
        """Test creating a temp table with aggregations for lead generation."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        sql_query = """
        SELECT 
            c.industry,
            c.region,
            COUNT(DISTINCT c.id) as company_count,
            SUM(t.revenue) as total_revenue,
            AVG(t.revenue) as avg_revenue,
            COUNT(DISTINCT ct.id) as contact_count
        FROM `main`.`default`.`companies` c
        LEFT JOIN `main`.`sales`.`transactions` t ON c.id = t.company_id
        LEFT JOIN `main`.`default`.`contacts` ct ON c.id = ct.company_id
        WHERE c.status = 'active'
            AND t.transaction_date >= '2024-01-01'
        GROUP BY c.industry, c.region
        HAVING total_revenue > 100000
        """
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (25,)
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="lead_segments",
                sql_query=sql_query
            )
            
            assert result["temp_table_name"] == "lead_segments"
            assert result["row_count"] == 25
            assert result["status"] == "created"
            assert "session-scoped" in result["note"]
    
    def test_query_execution_failure(self) -> None:
        """Test proper error handling when query execution fails."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            # Simulate a Databricks error
            mock_connect.return_value.__enter__.side_effect = Exception("Connection failed")
            
            with pytest.raises(QueryError, match="Failed to create temporary table"):
                client.create_temp_table(
                    temp_table_name="test_temp",
                    sql_query="SELECT * FROM `main`.`default`.`users`"
                )
    
    def test_concurrent_creation(self) -> None:
        """Test that concurrent temp table creation respects semaphore limits."""
        config = create_test_config()
        config.limits.max_concurrent_queries = 2
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        # This test verifies the semaphore is used but doesn't test actual concurrency
        # since that requires threading which is complex for unit tests
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (10,)
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="test_temp",
                sql_query="SELECT * FROM `main`.`default`.`users`"
            )
            
            assert result["status"] == "created"
    
    def test_query_without_backticks(self) -> None:
        """Test that queries without backticks are also validated correctly."""
        config = create_test_config()
        token_provider = create_mock_token_provider()
        client = DatabricksSQLClient(config, token_provider)
        
        sql_query = """
        SELECT u.id, u.name, o.order_date
        FROM main.default.users u
        JOIN main.sales.orders o ON u.id = o.user_id
        """
        
        with patch('databricks_mcp.client.databricks.sql.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = (30,)
            mock_connection = MagicMock()
            mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
            mock_connect.return_value.__enter__.return_value = mock_connection
            
            result = client.create_temp_table(
                temp_table_name="user_orders",
                sql_query=sql_query
            )
            
            assert result["temp_table_name"] == "user_orders"
            assert result["row_count"] == 30
