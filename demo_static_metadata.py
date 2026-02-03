#!/usr/bin/env python3
"""
Demo script showing static metadata feature.

This script demonstrates:
1. Loading static metadata from CSV files
2. Merging with Databricks-like schema information
3. Accessing metadata without querying Databricks
"""

import csv
from pathlib import Path
from databricks_mcp.metadata_loader import MetadataLoader, merge_metadata


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def create_demo_metadata():
    """Create demo metadata directory with sample CSV."""
    metadata_dir = Path("./metadata_demo")
    catalog_dir = metadata_dir / "main" / "default"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    
    # Create sample metadata file
    csv_file = catalog_dir / "customers.csv"
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
            "column_name": "customer_id",
            "data_type": "BIGINT",
            "description": "Unique customer identifier",
            "business_definition": "Primary key for customer records, generated sequentially",
            "example_values": "1001, 1002, 1003",
            "constraints": "NOT NULL PRIMARY KEY",
            "source_system": "CRM System (Salesforce)",
            "owner": "Customer Data Team",
            "tags": "pii,key,dimension",
        })
        writer.writerow({
            "column_name": "email",
            "data_type": "STRING",
            "description": "Customer's primary email address",
            "business_definition": "Primary contact email for marketing and notifications",
            "example_values": "customer@example.com, user@company.com",
            "constraints": "NOT NULL UNIQUE",
            "source_system": "CRM System (Salesforce)",
            "owner": "Customer Data Team",
            "tags": "pii,contact,gdpr",
        })
        writer.writerow({
            "column_name": "loyalty_points",
            "data_type": "INT",
            "description": "Customer's current loyalty points balance",
            "business_definition": "Cumulative points earned from purchases, 1 point per $1 spent",
            "example_values": "0, 250, 1500",
            "constraints": "DEFAULT 0 CHECK >= 0",
            "source_system": "Loyalty Platform",
            "owner": "Marketing Team",
            "tags": "business_metric,loyalty",
        })
    
    print(f"✓ Created demo metadata in: {metadata_dir}")
    print(f"✓ Sample CSV file: {csv_file}")
    return metadata_dir


def demo_static_metadata_only():
    """Demonstrate loading static metadata without Databricks."""
    print_section("Demo 1: Loading Static Metadata Only")
    
    # Create demo metadata
    metadata_dir = create_demo_metadata()
    
    # Initialize loader
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    # Load metadata
    print("Loading metadata for: main.default.customers\n")
    metadata = loader.get_table_metadata("main", "default", "customers")
    
    if metadata:
        print(f"Found {len(metadata)} columns with documentation:\n")
        for col in metadata:
            print(f"  📋 Column: {col['column_name']}")
            print(f"     Type: {col.get('data_type', 'N/A')}")
            print(f"     Description: {col.get('description', 'N/A')}")
            print(f"     Business: {col.get('business_definition', 'N/A')}")
            print(f"     Owner: {col.get('owner', 'N/A')}")
            print(f"     Tags: {col.get('tags', 'N/A')}")
            print()
    else:
        print("❌ No metadata found")
    
    # Clean up
    import shutil
    shutil.rmtree(metadata_dir)


def demo_merged_metadata():
    """Demonstrate merging static metadata with Databricks schema."""
    print_section("Demo 2: Merging Static Metadata with Databricks Schema")
    
    # Create demo metadata
    metadata_dir = create_demo_metadata()
    
    # Initialize loader
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    # Simulate Databricks column information
    databricks_columns = [
        {
            "name": "customer_id",
            "data_type": "BIGINT",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 1,
        },
        {
            "name": "email",
            "data_type": "STRING",
            "nullable": "NO",
            "comment": None,
            "ordinal_position": 2,
        },
        {
            "name": "loyalty_points",
            "data_type": "INT",
            "nullable": "YES",
            "comment": None,
            "ordinal_position": 3,
        },
        {
            "name": "created_at",
            "data_type": "TIMESTAMP",
            "nullable": "NO",
            "comment": "Record creation time",
            "ordinal_position": 4,
        },
    ]
    
    print("Databricks Schema (Live from warehouse):")
    for col in databricks_columns:
        print(f"  - {col['name']}: {col['data_type']} ({'NOT NULL' if col['nullable'] == 'NO' else 'NULL'})")
    print()
    
    # Load static metadata
    static_metadata = loader.get_table_metadata("main", "default", "customers")
    print(f"Static Metadata (From CSV): {len(static_metadata)} columns documented\n")
    
    # Merge
    merged = merge_metadata(databricks_columns, static_metadata)
    
    print("Merged Result (Databricks + Static):\n")
    for col in merged:
        print(f"  🎯 {col['name']}")
        print(f"     Type: {col['data_type']} (from Databricks)")
        print(f"     Nullable: {col['nullable']} (from Databricks)")
        if col.get('description'):
            print(f"     📝 Description: {col['description']} (from CSV)")
        if col.get('business_definition'):
            print(f"     💼 Business: {col['business_definition']} (from CSV)")
        if col.get('owner'):
            print(f"     👤 Owner: {col['owner']} (from CSV)")
        if col.get('tags'):
            print(f"     🏷️  Tags: {col['tags']} (from CSV)")
        print()
    
    # Clean up
    import shutil
    shutil.rmtree(metadata_dir)


def demo_cache_behavior():
    """Demonstrate metadata caching."""
    print_section("Demo 3: Metadata Caching Behavior")
    
    # Create demo metadata
    metadata_dir = create_demo_metadata()
    
    # Initialize loader
    loader = MetadataLoader(metadata_dir=metadata_dir, enabled=True)
    
    # First load
    print("First load (from CSV file)...")
    metadata1 = loader.get_table_metadata("main", "default", "customers")
    print(f"✓ Loaded {len(metadata1)} columns\n")
    
    # Second load (from cache)
    print("Second load (from cache)...")
    metadata2 = loader.get_table_metadata("main", "default", "customers")
    print(f"✓ Retrieved {len(metadata2)} columns")
    print(f"✓ Same object reference: {metadata1 is metadata2}\n")
    
    # Clear cache
    print("Clearing cache...")
    loader.clear_cache()
    print("✓ Cache cleared\n")
    
    # Third load (from file again)
    print("Third load (from CSV file again)...")
    metadata3 = loader.get_table_metadata("main", "default", "customers")
    print(f"✓ Loaded {len(metadata3)} columns")
    print(f"✓ Different object reference: {metadata1 is not metadata3}\n")
    
    # Clean up
    import shutil
    shutil.rmtree(metadata_dir)


def main():
    """Run all demos."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  Static Metadata Ingestion - Feature Demonstration".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝")
    
    try:
        # Run demos
        demo_static_metadata_only()
        demo_merged_metadata()
        demo_cache_behavior()
        
        # Success
        print_section("✅ All Demos Completed Successfully!")
        print("Key Takeaways:")
        print("  • Static metadata provides rich business context")
        print("  • Seamlessly merges with Databricks technical metadata")
        print("  • Intelligent caching improves performance")
        print("  • No Databricks queries needed for documentation access")
        print()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
