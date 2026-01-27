from __future__ import annotations

import re
from typing import Iterable

from .config import LimitsConfig, ScopeConfig
from .errors import GuardrailError, ScopeError

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sanitize_identifier(identifier: str, field_name: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise GuardrailError(f"Invalid identifier for {field_name}")
    return identifier


def ensure_catalog_allowed(catalog: str, scopes: ScopeConfig) -> None:
    if catalog not in scopes.catalogs:
        raise ScopeError(f"Catalog {catalog} is not allowlisted")


def ensure_schema_allowed(catalog: str, schema: str, scopes: ScopeConfig) -> None:
    allowed_schemas = scopes.catalogs.get(catalog, [])
    if schema not in allowed_schemas:
        raise ScopeError(f"Schema {schema} is not allowlisted for catalog {catalog}")


def ensure_statement_allowed(statement_type: str, allowed: Iterable[str]) -> None:
    normalized = statement_type.upper()
    if normalized not in allowed:
        raise GuardrailError(f"Statement type {normalized} is not allowed")


def detect_statement_type(sql: str) -> str:
    stripped = sql.strip().split()
    if not stripped:
        raise GuardrailError("SQL statement is empty")
    return stripped[0].upper()


def clamp_limit(requested: int | None, cap: int) -> int | None:
    if cap == -1:
        return requested
    if requested is None:
        return cap
    return min(requested, cap)


def effective_timeout(requested: int | None, config: LimitsConfig) -> int | None:
    if requested is None:
        return (
            config.query_timeout_seconds if config.query_timeout_seconds != -1 else None
        )
    if config.query_timeout_seconds == -1:
        return requested
    return min(requested, config.query_timeout_seconds)


def validate_temp_table_query(sql: str, scopes: ScopeConfig) -> None:
    """
    Validate a SQL query intended for temporary table creation.
    
    Ensures:
    - Query is a SELECT statement (reads only, no writes)
    - All referenced catalogs and schemas are in allowlist
    - No obvious attempts to bypass guardrails
    
    Parameters:
    sql (str): SQL query to validate
    scopes (ScopeConfig): Configured catalog/schema allowlist
    
    Raises:
    GuardrailError: If validation fails
    """
    # Remove leading/trailing whitespace and normalize
    normalized = sql.strip()
    
    # Ensure it's a SELECT statement
    if not normalized.upper().startswith("SELECT"):
        raise GuardrailError(
            "Temporary table query must be a SELECT statement for privacy-first design"
        )
    
    # Check for forbidden keywords that might bypass guardrails
    forbidden_patterns = [
        r'\bINTO\s+OUTFILE\b',
        r'\bINTO\s+DUMPFILE\b',
        r'\bLOAD_FILE\b',
        r'\bEXEC\b',
        r'\bEXECUTE\b',
        r'\bCALL\b',
    ]
    
    normalized_upper = normalized.upper()
    for pattern in forbidden_patterns:
        if re.search(pattern, normalized_upper, re.IGNORECASE):
            raise GuardrailError(f"Query contains forbidden pattern: {pattern}")
    
    # Extract catalog references using backticks (Databricks style: `catalog`.`schema`.`table`)
    # Pattern matches: `catalog`.`schema`.`table` or catalog.schema.table
    catalog_pattern = r'`?(\w+)`?\.`?(\w+)`?\.`?\w+`?'
    matches = re.findall(catalog_pattern, normalized)
    
    # Validate all referenced catalogs and schemas are in allowlist
    for catalog, schema in matches:
        if catalog not in scopes.catalogs:
            raise GuardrailError(
                f"Query references catalog '{catalog}' which is not in allowlist"
            )
        allowed_schemas = scopes.catalogs.get(catalog, [])
        if schema not in allowed_schemas:
            raise GuardrailError(
                f"Query references schema '{schema}' in catalog '{catalog}' which is not in allowlist"
            )
