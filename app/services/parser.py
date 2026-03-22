from __future__ import annotations

import re
from typing import Final

_SPLIT_REGEX: Final[str] = r"(?<!http)(?<!https)(?<!android):"


def _sanitize_text(value: str) -> str:
    # PostgreSQL TEXT cannot contain NUL bytes.
    return value.replace("\x00", "").strip()


def parse_combo_line(raw_line: str) -> tuple[str, str, str] | None:
    line = _sanitize_text(raw_line)

    if not line or "@@" in line or len(line) < 5:
        return None

    if "," in line:
        first = line.find(",")
        if first <= 0:
            return None
        second = line.find(",", first + 1)
        if second <= first + 1:
            return None
        url = _sanitize_text(line[:first])
        username = _sanitize_text(line[first + 1 : second])
        password = _sanitize_text(line[second + 1 :])
    else:
        parts = re.split(_SPLIT_REGEX, line)
        if len(parts) < 3:
            return None
        url = _sanitize_text(parts[0])
        username = _sanitize_text(parts[1])
        password = _sanitize_text(":".join(parts[2:]))

    if not url or not username or not password:
        return None

    return url, username, password


def format_combo_entry(url: str, username: str, password: str) -> str:
    return f"{_sanitize_text(url)},{_sanitize_text(username)},{_sanitize_text(password)}"
