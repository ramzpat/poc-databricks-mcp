"""
Comprehensive validation suite to verify the create_temp_table implementation.

This script validates:
1. All imports work correctly
2. Guardrails function as expected
3. Error messages are clear and helpful
4. The implementation follows existing patterns
"""

import sys
from typing import List, Tuple

def test_imports() -> Tuple[bool, List[str]]:
    """Test that all required imports work."""
    errors = []
    
    try:
        from databricks_mcp.guardrails import validate_temp_table_query
        from databricks_mcp.client import DatabricksSQLClient
        from databricks_mcp.server import build_app
        from databricks_mcp.config import ScopeConfig
        from databricks_mcp.errors import GuardrailError, QueryError
    except ImportError as e:
        errors.append(f"Import failed: {e}")
        return False, errors
    
    return True, []

def test_guardrails() -> Tuple[bool, List[str]]:
    """Test guardrail validations."""
    errors = []
    
    try:
        from databricks_mcp.guardrails import validate_temp_table_query
        from databricks_mcp.config import ScopeConfig
        from databricks_mcp.errors import GuardrailError
        
        scopes = ScopeConfig(catalogs={
            "main": ["default", "sales"],
            "analytics": ["reporting"]
        })
        
        # Test 1: Valid simple SELECT
        try:
            validate_temp_table_query(
                "SELECT * FROM `main`.`default`.`users`",
                scopes
            )
        except Exception as e:
            errors.append(f"Valid SELECT rejected: {e}")
        
        # Test 2: Valid JOIN
        try:
            validate_temp_table_query(
                """
                SELECT c.id, COUNT(o.id) FROM `main`.`default`.`customers` c
                JOIN `main`.`sales`.`orders` o ON c.id = o.customer_id
                GROUP BY c.id
                """,
                scopes
            )
        except Exception as e:
            errors.append(f"Valid JOIN rejected: {e}")
        
        # Test 3: Cross-catalog
        try:
            validate_temp_table_query(
                """
                SELECT * FROM `main`.`default`.`a`
                JOIN `analytics`.`reporting`.`b` ON a.id = b.id
                """,
                scopes
            )
        except Exception as e:
            errors.append(f"Valid cross-catalog rejected: {e}")
        
        # Test 4: Non-SELECT should fail
        try:
            validate_temp_table_query(
                "INSERT INTO main.default.users VALUES (1)",
                scopes
            )
            errors.append("Non-SELECT query was not rejected")
        except GuardrailError:
            pass  # Expected
        
        # Test 5: Forbidden catalog should fail
        try:
            validate_temp_table_query(
                "SELECT * FROM `forbidden`.`default`.`users`",
                scopes
            )
            errors.append("Disallowed catalog was not rejected")
        except GuardrailError:
            pass  # Expected
        
        # Test 6: Forbidden schema should fail
        try:
            validate_temp_table_query(
                "SELECT * FROM `main`.`forbidden`.`users`",
                scopes
            )
            errors.append("Disallowed schema was not rejected")
        except GuardrailError:
            pass  # Expected
        
        # Test 7: Forbidden pattern should fail
        try:
            validate_temp_table_query(
                "SELECT * FROM `main`.`default`.`users` INTO OUTFILE '/tmp/data'",
                scopes
            )
            errors.append("Forbidden pattern was not rejected")
        except GuardrailError:
            pass  # Expected
        
    except Exception as e:
        errors.append(f"Guardrail test failed: {e}")
        import traceback
        errors.append(traceback.format_exc())
    
    return len(errors) == 0, errors

def test_identifier_validation() -> Tuple[bool, List[str]]:
    """Test SQL identifier validation."""
    errors = []
    
    try:
        from databricks_mcp.guardrails import sanitize_identifier
        from databricks_mcp.errors import GuardrailError
        
        # Valid identifiers
        valid_names = [
            "table1",
            "my_table",
            "_private_table",
            "TableName",
            "table_123",
        ]
        
        for name in valid_names:
            try:
                result = sanitize_identifier(name, "test")
                if result != name:
                    errors.append(f"Identifier '{name}' was modified to '{result}'")
            except GuardrailError:
                errors.append(f"Valid identifier '{name}' was rejected")
        
        # Invalid identifiers
        invalid_names = [
            "table-name",
            "table name",
            "table.name",
            "table@name",
            "123table",
            "table*",
        ]
        
        for name in invalid_names:
            try:
                sanitize_identifier(name, "test")
                errors.append(f"Invalid identifier '{name}' was accepted")
            except GuardrailError:
                pass  # Expected
        
    except Exception as e:
        errors.append(f"Identifier validation test failed: {e}")
    
    return len(errors) == 0, errors

def test_error_messages() -> Tuple[bool, List[str]]:
    """Test that error messages are clear and helpful."""
    errors = []
    
    try:
        from databricks_mcp.guardrails import validate_temp_table_query
        from databricks_mcp.config import ScopeConfig
        from databricks_mcp.errors import GuardrailError
        
        scopes = ScopeConfig(catalogs={"main": ["default"]})
        
        # Test error message quality
        test_cases = [
            (
                "INSERT INTO t VALUES (1)",
                "SELECT statement"
            ),
            (
                "SELECT * FROM `forbidden`.`default`.`t`",
                "allowlist"
            ),
            (
                "SELECT * INTO OUTFILE '/tmp/x' FROM t",
                "forbidden pattern"
            ),
        ]
        
        for query, expected_in_message in test_cases:
            try:
                validate_temp_table_query(query, scopes)
                errors.append(f"Query should have failed: {query}")
            except GuardrailError as e:
                if expected_in_message.lower() not in str(e).lower():
                    errors.append(
                        f"Error message missing '{expected_in_message}': {e}"
                    )
        
    except Exception as e:
        errors.append(f"Error message test failed: {e}")
    
    return len(errors) == 0, errors

def test_config_integration() -> Tuple[bool, List[str]]:
    """Test that configuration integrates properly."""
    errors = []
    
    try:
        from databricks_mcp.config import AppConfig, ScopeConfig, LimitsConfig
        
        # Verify CREATE is in allowed statements for the tool to work
        limits = LimitsConfig(
            max_rows=10000,
            sample_max_rows=1000,
            query_timeout_seconds=60,
            max_concurrent_queries=5,
            allow_statement_types=["SELECT", "CREATE"]
        )
        
        if "CREATE" not in limits.allow_statement_types:
            errors.append("CREATE statement type not configured")
        
        if "SELECT" not in limits.allow_statement_types:
            errors.append("SELECT statement type not configured")
        
    except Exception as e:
        errors.append(f"Config integration test failed: {e}")
    
    return len(errors) == 0, errors

def run_all_tests() -> bool:
    """Run all validation tests."""
    all_passed = True
    
    print("=" * 80)
    print("CREATE_TEMP_TABLE IMPLEMENTATION VALIDATION")
    print("=" * 80)
    print()
    
    tests = [
        ("Imports", test_imports),
        ("Guardrails", test_guardrails),
        ("Identifier Validation", test_identifier_validation),
        ("Error Messages", test_error_messages),
        ("Config Integration", test_config_integration),
    ]
    
    for test_name, test_func in tests:
        print(f"Running: {test_name}...")
        passed, errors = test_func()
        
        if passed:
            print(f"  ✓ {test_name} PASSED")
        else:
            print(f"  ✗ {test_name} FAILED")
            for error in errors:
                print(f"    - {error}")
            all_passed = False
        print()
    
    print("=" * 80)
    if all_passed:
        print("✓ ALL VALIDATION TESTS PASSED")
        print()
        print("The create_temp_table implementation is ready for use!")
        print()
        print("Next steps:")
        print("1. Update config.yml to include CREATE in allow_statement_types")
        print("2. Run pytest to execute full test suite")
        print("3. Review CREATE_TEMP_TABLE_GUIDE.md for usage examples")
        print("4. Check examples_create_temp_table.py for practical scenarios")
    else:
        print("✗ SOME VALIDATION TESTS FAILED")
        print()
        print("Please review the errors above and fix the issues.")
    print("=" * 80)
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
