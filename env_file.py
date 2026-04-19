from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _unquote_env_value(value.strip())
        os.environ.setdefault(key, value)


def write_env_values(path: Path, updates: dict[str, str]) -> None:
    existing: dict[str, str] = {}
    lines: list[str] = []

    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            existing[key.strip()] = _unquote_env_value(value.strip())

    existing.update(updates)

    rendered_keys: set[str] = set()
    rendered_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            rendered_lines.append(line)
            continue
        key, _value = stripped.split("=", 1)
        key = key.strip()
        if key in existing:
            rendered_lines.append(f"{key}={_quote_env_value(existing[key])}")
            rendered_keys.add(key)
        else:
            rendered_lines.append(line)

    for key in sorted(existing):
        if key not in rendered_keys:
            rendered_lines.append(f"{key}={_quote_env_value(existing[key])}")

    content = "\n".join(rendered_lines).rstrip() + "\n"
    path.write_text(content, encoding="utf-8")


def _quote_env_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _unquote_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
