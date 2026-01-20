import os
from pathlib import Path

import pytest

from databricks_mcp.config import load_config
from databricks_mcp.errors import ConfigError


def write_config(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "config.yml"
    path.write_text(content)
    return path


def test_env_substitution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "secret-value")
    cfg_path = write_config(
        tmp_path,
        """
warehouse:
  host: https://example.cloud.databricks.com
  http_path: /sql/1.0/warehouses/abc
  warehouse_id: warehouse-123
auth:
  oauth:
    client_id: client-id
    client_secret: ${DATABRICKS_CLIENT_SECRET}
    token_url: https://example.cloud.databricks.com/oidc/v1/token
scopes:
  catalogs: [main]
  schemas: [default]
limits:
  max_rows: 100
  sample_max_rows: 10
  query_timeout_seconds: 5
  max_concurrent_queries: 2
  allow_statement_types: [SELECT]
""",
    )

    config = load_config(cfg_path)
    assert config.oauth.client_secret == "secret-value"
    assert config.limits.allow_statement_types == ["SELECT"]


def test_missing_required_section(tmp_path: Path) -> None:
    cfg_path = write_config(
        tmp_path,
        """
warehouse:
  host: https://example.cloud.databricks.com
  http_path: /sql/1.0/warehouses/abc
  warehouse_id: warehouse-123
scopes:
  catalogs: [main]
  schemas: [default]
""",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_path)


def test_invalid_limits(tmp_path: Path) -> None:
    cfg_path = write_config(
        tmp_path,
        """
warehouse:
  host: https://example.cloud.databricks.com
  http_path: /sql/1.0/warehouses/abc
  warehouse_id: warehouse-123
auth:
  oauth:
    client_id: client-id
    client_secret: secret
    token_url: https://example.cloud.databricks.com/oidc/v1/token
scopes:
  catalogs: [main]
  schemas: [default]
limits:
  max_rows: 0
  sample_max_rows: 0
  query_timeout_seconds: 0
  max_concurrent_queries: -1
""",
    )
    with pytest.raises(ConfigError):
        load_config(cfg_path)
