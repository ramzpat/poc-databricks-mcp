#!/usr/bin/env python3
"""
Quick validation script to check if the implementation is syntactically correct
and imports work.
"""

import sys

try:
    # Test imports
    from databricks_mcp.guardrails import validate_temp_table_query
    from databricks_mcp.client import DatabricksSQLClient
    from databricks_mcp.server import build_app
    from databricks_mcp.config import ScopeConfig
    
    print("✓ All imports successful")
    
    # Test basic validation
    scopes = ScopeConfig(catalogs={"main": ["default"]})
    
    # Valid query
    try:
        validate_temp_table_query("SELECT * FROM `main`.`default`.`users`", scopes)
        print("✓ Valid query validation passed")
    except Exception as e:
        print(f"✗ Valid query validation failed: {e}")
        sys.exit(1)
    
    # Invalid query (non-SELECT)
    try:
        validate_temp_table_query("INSERT INTO main.default.users VALUES (1)", scopes)
        print("✗ Invalid query should have been rejected")
        sys.exit(1)
    except Exception:
        print("✓ Invalid query correctly rejected")
    
    # Invalid catalog
    try:
        validate_temp_table_query("SELECT * FROM `forbidden`.`default`.`users`", scopes)
        print("✗ Disallowed catalog should have been rejected")
        sys.exit(1)
    except Exception:
        print("✓ Disallowed catalog correctly rejected")
    
    print("\n✓ All validation checks passed!")
    print("\nImplementation is ready for use.")
    
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
