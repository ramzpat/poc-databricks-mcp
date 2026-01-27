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
    # Allow global_temp catalog for temporary tables
    if catalog == "global_temp":
        return
    if catalog not in scopes.catalogs:
        raise ScopeError(f"Catalog {catalog} is not allowlisted")


def ensure_schema_allowed(catalog: str, schema: str, scopes: ScopeConfig) -> None:
    # Allow global_temp catalog without schema validation (temp tables don't use schemas)
    if catalog == "global_temp":
        return
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
