from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import ConfigError


@dataclass
class WarehouseConfig:
    host: str
    http_path: str
    warehouse_id: str


@dataclass
class OAuthConfig:
    client_id: str
    client_secret: str
    token_url: str
    scope: str | None = None


@dataclass
class ScopeConfig:
    catalogs: list[str]
    schemas: list[str]


@dataclass
class LimitsConfig:
    max_rows: int
    sample_max_rows: int
    query_timeout_seconds: int
    max_concurrent_queries: int
    allow_statement_types: list[str]


@dataclass
class ObservabilityConfig:
    log_level: str = "info"
    propagate_request_ids: bool = True


@dataclass
class AppConfig:
    warehouse: WarehouseConfig
    oauth: OAuthConfig
    scopes: ScopeConfig
    limits: LimitsConfig
    observability: ObservabilityConfig


_ALLOWED_STATEMENTS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CREATE",
    "ALTER",
    "DROP",
}


def _resolve_env(value: Any, env: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        expanded = os.path.expandvars(value)
        if expanded.startswith("${") and expanded.endswith("}"):
            key = expanded[2:-1]
            if key not in env:
                raise ConfigError(f"Environment variable {key} is required but not set")
            return env[key]
        return expanded
    if isinstance(value, list):
        return [_resolve_env(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _resolve_env(v, env) for k, v in value.items()}
    return value


def _validate_positive_or_unlimited(value: int, field_name: str) -> int:
    if value == -1:
        return value
    if value <= 0:
        raise ConfigError(f"{field_name} must be greater than 0 or -1 for no limit")
    return value


def _normalize_statements(statements: list[str] | None) -> list[str]:
    normalized = [s.upper() for s in (statements or ["SELECT"])]
    for stmt in normalized:
        if stmt not in _ALLOWED_STATEMENTS:
            raise ConfigError(f"Unsupported statement type: {stmt}")
    return list(dict.fromkeys(normalized))


def load_config(path: str | Path, env: Mapping[str, str] | None = None) -> AppConfig:
    env = env or os.environ
    raw = yaml.safe_load(Path(path).read_text()) or {}
    resolved = _resolve_env(raw, env)

    try:
        warehouse_raw = resolved["warehouse"]
        auth_raw = resolved.get("auth", {})
        oauth_raw = auth_raw.get("oauth", {})
        scopes_raw = resolved["scopes"]
        limits_raw = resolved.get("limits", {})
        observability_raw = resolved.get("observability", {})
    except KeyError as exc:
        raise ConfigError(f"Missing config section: {exc.args[0]}") from exc

    warehouse = WarehouseConfig(
        host=warehouse_raw["host"],
        http_path=warehouse_raw["http_path"],
        warehouse_id=warehouse_raw["warehouse_id"],
    )

    oauth = OAuthConfig(
        client_id=oauth_raw["client_id"],
        client_secret=oauth_raw["client_secret"],
        token_url=oauth_raw["token_url"],
        scope=oauth_raw.get("scope"),
    )

    catalogs = scopes_raw.get("catalogs", [])
    schemas = scopes_raw.get("schemas", [])
    if not catalogs:
        raise ConfigError("At least one catalog must be allowlisted")
    if not schemas:
        raise ConfigError("At least one schema must be allowlisted")
    scopes = ScopeConfig(catalogs=catalogs, schemas=schemas)

    limits = LimitsConfig(
        max_rows=_validate_positive_or_unlimited(limits_raw.get("max_rows", 10000), "max_rows"),
        sample_max_rows=_validate_positive_or_unlimited(limits_raw.get("sample_max_rows", 1000), "sample_max_rows"),
        query_timeout_seconds=_validate_positive_or_unlimited(
            limits_raw.get("query_timeout_seconds", 60), "query_timeout_seconds"
        ),
        max_concurrent_queries=_validate_positive_or_unlimited(
            limits_raw.get("max_concurrent_queries", 5), "max_concurrent_queries"
        ),
        allow_statement_types=_normalize_statements(limits_raw.get("allow_statement_types")),
    )

    observability = ObservabilityConfig(
        log_level=str(observability_raw.get("log_level", "info")),
        propagate_request_ids=bool(observability_raw.get("propagate_request_ids", True)),
    )

    if not warehouse.host or not warehouse.http_path or not warehouse.warehouse_id:
        raise ConfigError("Warehouse host, http_path, and warehouse_id are required")
    if not oauth.client_id or not oauth.client_secret or not oauth.token_url:
        raise ConfigError("OAuth client_id, client_secret, and token_url are required")

    if limits.max_concurrent_queries == -1:
        raise ConfigError("max_concurrent_queries cannot be unlimited")

    return AppConfig(
        warehouse=warehouse,
        oauth=oauth,
        scopes=scopes,
        limits=limits,
        observability=observability,
    )
