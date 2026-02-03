"""Tests for static metadata loader functionality."""

import csv
from pathlib import Path

import pytest

from databricks_mcp.metadata_loader import MetadataLoader, merge_metadata


@pytest.fixture
def metadata_dir(tmp_path: Path) -> Path:
    """Create a temporary metadata directory structure."""
    metadata_root = tmp_path / "metadata"
    catalog_dir = metadata_root / "test_catalog" / "test_schema"
    catalog_dir.mkdir(parents=True)
    
    # Create a sample metadata CSV file
    csv_file = catalog_dir / "test_table.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "column_name",
                "data_type",
                "description",
                "business_definition",
                "example_values",
                "constraints",
                "source_system",
                "owner",
                "tags",
            ],
        )
        writer.writeheader()
        writer.writerow({
            "column_name": "user_id",
            "data_type": "BIGINT",
            "description": "Unique identifier for users",
            "business_definition": "Primary key for user records",
            "example_values": "12345, 67890",
            "constraints": "NOT NULL PRIMARY KEY",
            "source_system": "CRM System",
            "owner": "Data Engineering",
            "tags": "pii,key",
        })
        writer.writerow({
            "column_name": "username",
            "data_type": "STRING",
            "description": "User's login name",
            "business_definition": "Unique username for authentication",
            "example_values": "john_doe, jane_smith",
            "constraints": "NOT NULL UNIQUE",
            "source_system": "CRM System",
            "owner": "Data Engineering",
            "tags": "pii",
        })
    
    return metadata_root


def test_metadata_loader_disabled():
    """Test that MetadataLoader works when disabled."""
    loader = MetadataLoader(metadata_dir=None, enabled=False)
    assert not loader.is_enabled()
    assert loader.get_table_metadata("catalog", "schema", "table") is None


def test_metadata_loader_enabled_no_directory():
    """Test that MetadataLoader handles missing directory."""
    loader = MetadataLoader(metadata_dir="/nonexistent/path", enabled=True)
    assert not loader.is_enabled()


def test_metadata_loader_load_existing(metadata_dir: Path):
    """Test loading existing metadata."""
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    assert loader.is_enabled()
    
    metadata = loader.get_table_metadata("test_catalog", "test_schema", "test_table")
    assert metadata is not None
    assert len(metadata) == 2
    assert metadata[0]["column_name"] == "user_id"
    assert metadata[0]["description"] == "Unique identifier for users"
    assert metadata[1]["column_name"] == "username"


def test_metadata_loader_missing_table(metadata_dir: Path):
    """Test loading metadata for non-existent table."""
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    metadata = loader.get_table_metadata("test_catalog", "test_schema", "missing_table")
    assert metadata is None


def test_metadata_loader_cache(metadata_dir: Path):
    """Test that metadata is cached."""
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    # First load
    metadata1 = loader.get_table_metadata("test_catalog", "test_schema", "test_table")
    # Second load (should use cache)
    metadata2 = loader.get_table_metadata("test_catalog", "test_schema", "test_table")
    
    assert metadata1 is metadata2  # Same object reference


def test_metadata_loader_clear_cache(metadata_dir: Path):
    """Test cache clearing."""
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    metadata1 = loader.get_table_metadata("test_catalog", "test_schema", "test_table")
    loader.clear_cache()
    metadata2 = loader.get_table_metadata("test_catalog", "test_schema", "test_table")
    
    assert metadata1 is not metadata2  # Different object references


def test_merge_metadata_no_static():
    """Test merging when there's no static metadata."""
    databricks_columns = [
        {
            "name": "user_id",
            "data_type": "BIGINT",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 1,
        },
        {
            "name": "username",
            "data_type": "STRING",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 2,
        },
    ]
    
    merged = merge_metadata(databricks_columns, None)
    assert merged == databricks_columns


def test_merge_metadata_with_static():
    """Test merging Databricks metadata with static metadata."""
    databricks_columns = [
        {
            "name": "user_id",
            "data_type": "BIGINT",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 1,
        },
        {
            "name": "username",
            "data_type": "STRING",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 2,
        },
    ]
    
    static_metadata = [
        {
            "column_name": "user_id",
            "description": "Unique identifier for users",
            "business_definition": "Primary key",
            "example_values": "12345",
            "constraints": "NOT NULL",
            "source_system": "CRM",
            "owner": "Engineering",
            "tags": "pii",
        },
    ]
    
    merged = merge_metadata(databricks_columns, static_metadata)
    
    # Check that static metadata is added to user_id
    assert merged[0]["name"] == "user_id"
    assert merged[0]["data_type"] == "BIGINT"  # From Databricks
    assert merged[0]["nullable"] == "NO"  # From Databricks
    assert merged[0]["description"] == "Unique identifier for users"  # From static
    assert merged[0]["business_definition"] == "Primary key"  # From static
    assert merged[0]["example_values"] == "12345"  # From static
    
    # Check that username has no extra fields (no static metadata)
    assert merged[1]["name"] == "username"
    assert "description" not in merged[1]


def test_merge_metadata_case_insensitive():
    """Test that column name matching is case-insensitive."""
    databricks_columns = [
        {
            "name": "UserId",  # Mixed case
            "data_type": "BIGINT",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 1,
        },
    ]
    
    static_metadata = [
        {
            "column_name": "userid",  # Lowercase
            "description": "User identifier",
        },
    ]
    
    merged = merge_metadata(databricks_columns, static_metadata)
    assert merged[0]["description"] == "User identifier"
