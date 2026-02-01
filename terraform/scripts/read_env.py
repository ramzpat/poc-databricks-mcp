from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple


def parse_env_line(line: str) -> Optional[Tuple[str, str]]:
    """Parse a single .env line into a key/value pair."""
    # Normalize the line and skip empty or comment-only content.
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()

    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()

    if not key:
        return None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]

    return key, value


def load_env(path: Path) -> Dict[str, str]:
    """Load a .env file into a dictionary of strings."""
    # Keep the last value for any duplicate keys.
    env: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = parse_env_line(line)
        if parsed is None:
            continue
        key, value = parsed
        env[key] = value

    return env


def main() -> int:
    """Read a .env file and output JSON for Terraform external data."""
    # Terraform external data sources expect JSON on stdout.
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: read_env.py <path-to-env>\n")
        return 1

    env_path = Path(sys.argv[1]).expanduser()
    if not env_path.exists():
        sys.stderr.write(f"Env file not found: {env_path}\n")
        return 1

    env = load_env(env_path)
    print(json.dumps(env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
