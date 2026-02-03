"""Integration test to demonstrate static metadata feature."""

from pathlib import Path
import tempfile
import csv

from databricks_mcp.config import load_config, AppConfig, MetadataConfig
from databricks_mcp.metadata_loader import MetadataLoader, merge_metadata


def test_end_to_end_metadata_flow():
    """Test the complete flow of loading and merging metadata."""
    # Create a temporary metadata directory
    with tempfile.TemporaryDirectory() as tmpdir:
        metadata_dir = Path(tmpdir) / "metadata"
        catalog_dir = metadata_dir / "test_catalog" / "test_schema"
        catalog_dir.mkdir(parents=True)
        
        # Create a sample CSV file
        csv_file = catalog_dir / "products.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "column_name",
                    "description",
                    "business_definition",
                    "example_values",
                    "owner",
                    "tags",
                ],
            )
            writer.writeheader()
            writer.writerow({
                "column_name": "product_id",
                "description": "Unique product identifier",
                "business_definition": "Primary key for product catalog",
                "example_values": "PROD-001, PROD-002",
                "owner": "Product Team",
                "tags": "key,pii",
            })
            writer.writerow({
                "column_name": "product_name",
                "description": "Product display name",
                "business_definition": "Customer-facing product name",
                "example_values": "Widget A, Gadget B",
                "owner": "Product Team",
                "tags": "customer_facing",
            })
            writer.writerow({
                "column_name": "price",
                "description": "Product price in USD",
                "business_definition": "Current retail price",
                "example_values": "19.99, 49.99",
                "owner": "Finance Team",
                "tags": "financial,currency_usd",
            })
        
        # Initialize MetadataLoader
        loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
        assert loader.is_enabled()
        
        # Load metadata
        metadata = loader.get_table_metadata(
            "test_catalog", "test_schema", "products"
        )
        assert metadata is not None
        assert len(metadata) == 3
        
        # Verify metadata content
        assert metadata[0]["column_name"] == "product_id"
        assert metadata[0]["description"] == "Unique product identifier"
        assert metadata[0]["owner"] == "Product Team"
        assert metadata[0]["tags"] == "key,pii"
        
        # Simulate Databricks columns
        databricks_columns = [
            {
                "name": "product_id",
                "data_type": "BIGINT",
                "nullable": "NO",
                "comment": None,
                "ordinal_position": 1,
            },
            {
                "name": "product_name",
                "data_type": "STRING",
                "nullable": "YES",
                "comment": None,
                "ordinal_position": 2,
            },
            {
                "name": "price",
                "data_type": "DECIMAL(10,2)",
                "nullable": "NO",
                "comment": None,
                "ordinal_position": 3,
            },
        ]
        
        # Merge metadata
        merged = merge_metadata(databricks_columns, metadata)
        
        # Verify merged data
        assert len(merged) == 3
        
        # Check first column has both Databricks and static metadata
        assert merged[0]["name"] == "product_id"
        assert merged[0]["data_type"] == "BIGINT"  # From Databricks
        assert merged[0]["nullable"] == "NO"  # From Databricks
        assert merged[0]["description"] == "Unique product identifier"  # From CSV
        assert merged[0]["business_definition"] == "Primary key for product catalog"  # From CSV
        assert merged[0]["owner"] == "Product Team"  # From CSV
        assert merged[0]["tags"] == "key,pii"  # From CSV
        
        # Check second column
        assert merged[1]["name"] == "product_name"
        assert merged[1]["data_type"] == "STRING"
        assert merged[1]["description"] == "Product display name"
        assert merged[1]["owner"] == "Product Team"
        
        # Check third column
        assert merged[2]["name"] == "price"
        assert merged[2]["data_type"] == "DECIMAL(10,2)"
        assert merged[2]["description"] == "Product price in USD"
        assert merged[2]["owner"] == "Finance Team"
        assert merged[2]["tags"] == "financial,currency_usd"
        
        print("\n✓ End-to-end metadata flow test passed!")
        print(f"✓ Loaded {len(metadata)} column metadata entries")
        print(f"✓ Successfully merged with Databricks schema")
        print(f"✓ All enrichment fields present in output")


def test_config_with_metadata():
    """Test configuration loading with metadata settings."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write("""
warehouse:
  host: https://test.databricks.com
  http_path: /sql/1.0/warehouses/abc
  warehouse_id: test-123
auth:
  oauth:
    client_id: test-client
    client_secret: test-secret
    token_url: https://test.databricks.com/token
scopes:
  catalogs:
    main:
      schemas: [default]
limits:
  max_rows: 1000
  sample_max_rows: 100
  query_timeout_seconds: 30
  max_concurrent_queries: 5
  allow_statement_types: [SELECT]
metadata:
  enabled: true
  directory: ./metadata
""")
        config_path = f.name
    
    try:
        config = load_config(config_path)
        
        # Verify metadata config loaded correctly
        assert isinstance(config.metadata, MetadataConfig)
        assert config.metadata.enabled is True
        assert config.metadata.directory == "./metadata"
        
        print("\n✓ Configuration with metadata settings loaded successfully!")
        print(f"✓ Metadata enabled: {config.metadata.enabled}")
        print(f"✓ Metadata directory: {config.metadata.directory}")
    finally:
        Path(config_path).unlink()


if __name__ == "__main__":
    test_end_to_end_metadata_flow()
    test_config_with_metadata()
    print("\n✅ All integration tests passed!")
