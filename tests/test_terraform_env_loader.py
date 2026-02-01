import importlib.util
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "terraform"
    / "scripts"
    / "read_env.py"
)

spec = importlib.util.spec_from_file_location("read_env", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

parse_env_line = module.parse_env_line
load_env = module.load_env


def test_parse_env_line_supports_export_and_quotes() -> None:
    """Verify export prefixes and quoted values are parsed correctly."""
    # Ensure export is stripped and quotes are removed.
    assert parse_env_line('export FOO="bar"') == ("FOO", "bar")


def test_load_env_ignores_comments_and_blanks(tmp_path: Path) -> None:
    """Ensure blank lines and comments are ignored when loading .env."""
    # Write a sample .env file for parsing.
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\n\nFOO=bar\nBAZ='qux'\n",
        encoding="utf-8"
    )

    env = load_env(env_file)

    assert env == {"FOO": "bar", "BAZ": "qux"}
