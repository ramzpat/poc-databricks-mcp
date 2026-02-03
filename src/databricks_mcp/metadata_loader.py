"""Static metadata loader for table metadata.

This module provides functionality to load and manage static metadata
from CSV files for Databricks tables.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any


class MetadataLoader:
    """Load and cache static table metadata from CSV files."""

    def __init__(self, metadata_dir: str | Path | None = None, enabled: bool = True):
        """Initialize the metadata loader.

        Args:
            metadata_dir: Directory containing metadata files. If None, metadata is disabled.
            enabled: Whether metadata loading is enabled.
        """
        self._enabled = enabled and metadata_dir is not None
        self._metadata_dir = Path(metadata_dir) if metadata_dir else None
        self._cache: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._log = logging.getLogger(__name__)

        if self._enabled and self._metadata_dir:
            if not self._metadata_dir.exists():
                self._log.warning(
                    f"Metadata directory does not exist: {self._metadata_dir}"
                )
                self._enabled = False

    def get_table_metadata(
        self, catalog: str, schema: str, table: str
    ) -> list[dict[str, Any]] | None:
        """Get static metadata for a table.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            List of column metadata dictionaries, or None if not found
        """
        if not self._enabled:
            return None

        cache_key = (catalog, schema, table)
        if cache_key in self._cache:
            return self._cache[cache_key]

        metadata = self._load_metadata(catalog, schema, table)
        if metadata:
            self._cache[cache_key] = metadata
        return metadata

    def _load_metadata(
        self, catalog: str, schema: str, table: str
    ) -> list[dict[str, Any]] | None:
        """Load metadata from CSV file.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            List of column metadata dictionaries, or None if file not found
        """
        if not self._metadata_dir:
            return None

        csv_path = self._metadata_dir / catalog / schema / f"{table}.csv"
        if not csv_path.exists():
            self._log.debug(f"No static metadata found for {catalog}.{schema}.{table}")
            return None

        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                metadata = list(reader)
                self._log.info(
                    f"Loaded {len(metadata)} column metadata entries for {catalog}.{schema}.{table}"
                )
                return metadata
        except Exception as exc:
            self._log.error(
                f"Failed to load metadata from {csv_path}: {exc}", exc_info=True
            )
            return None

    def clear_cache(self) -> None:
        """Clear the metadata cache."""
        self._cache.clear()

    def is_enabled(self) -> bool:
        """Check if metadata loading is enabled.

        Returns:
            True if metadata loading is enabled and directory is valid
        """
        return self._enabled


def merge_metadata(
    databricks_columns: list[dict[str, Any]],
    static_metadata: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Merge Databricks column metadata with static metadata.

    Static metadata takes precedence for documentation fields.
    Databricks metadata is used for technical schema details.

    Args:
        databricks_columns: Column metadata from Databricks
        static_metadata: Static column metadata from CSV files

    Returns:
        Merged column metadata
    """
    if not static_metadata:
        return databricks_columns

    # Create a lookup map for static metadata by column name
    static_map = {
        row.get("column_name", "").lower(): row for row in static_metadata if row.get("column_name")
    }

    merged = []
    for col in databricks_columns:
        col_name = col.get("name", "")
        merged_col = col.copy()

        # Look up static metadata for this column
        static_col = static_map.get(col_name.lower())
        if static_col:
            # Add or override with static metadata fields
            if static_col.get("description"):
                merged_col["description"] = static_col["description"]
            if static_col.get("business_definition"):
                merged_col["business_definition"] = static_col["business_definition"]
            if static_col.get("example_values"):
                merged_col["example_values"] = static_col["example_values"]
            if static_col.get("constraints"):
                merged_col["constraints"] = static_col["constraints"]
            if static_col.get("source_system"):
                merged_col["source_system"] = static_col["source_system"]
            if static_col.get("owner"):
                merged_col["owner"] = static_col["owner"]
            if static_col.get("tags"):
                merged_col["tags"] = static_col["tags"]

        merged.append(merged_col)

    return merged
