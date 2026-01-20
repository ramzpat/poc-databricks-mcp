class ConfigError(ValueError):
    """Configuration is missing or invalid."""


class AuthError(RuntimeError):
    """Authentication or token exchange failed."""


class ScopeError(PermissionError):
    """Requested catalog or schema is outside the allowlist."""


class GuardrailError(RuntimeError):
    """Query violates configured guardrails."""


class QueryError(RuntimeError):
    """Query execution failed in a user-facing way."""
