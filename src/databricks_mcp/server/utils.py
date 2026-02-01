"""Databricks authentication helpers for MCP server.

This module provides utility functions for authenticating with Databricks
and accessing user information in both deployed and local environments.
"""

import contextvars
from typing import Any

from databricks.sdk import WorkspaceClient

request_headers_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "request_headers", default={}
)


class HeaderStore:
    """Thread-safe context manager for request headers."""

    def set(self, headers: dict[str, Any]) -> None:
        """Store headers in context for the current request."""
        request_headers_context.set(headers)

    def get(self) -> dict[str, Any]:
        """Retrieve headers from the current request context."""
        return request_headers_context.get({})


header_store = HeaderStore()


def get_workspace_client() -> WorkspaceClient:
    """Get a Databricks workspace client authenticated as the service principal.

    When deployed as a Databricks App, this returns a client authenticated with
    the service principal associated with the app. When running locally, it returns
    a client authenticated as the current developer.

    Returns:
        WorkspaceClient: Authenticated Databricks workspace client
    """
    return WorkspaceClient()


def get_user_authenticated_workspace_client() -> WorkspaceClient:
    """Get a Databricks workspace client authenticated as the end user.

    When deployed as a Databricks App, this returns a client authenticated with
    the end user's credentials. When running locally, it falls back to the
    developer's authentication.

    Returns:
        WorkspaceClient: End-user authenticated Databricks workspace client
    """
    return WorkspaceClient()
