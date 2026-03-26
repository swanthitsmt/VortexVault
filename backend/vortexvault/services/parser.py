from __future__ import annotations

def _split_first_two(line: str, sep: str) -> tuple[str, str, str] | None:
    first = line.find(sep)
    if first <= 0:
        return None
    second = line.find(sep, first + 1)
    if second <= first + 1:
        return None
    left = line[:first].strip()
    mid = line[first + 1 : second].strip()
    right = line[second + 1 :].strip()
    if not left or not mid or not right:
        return None
    return left, mid, right


def parse_combo_line(raw: bytes) -> tuple[str, str, str] | None:
    # Ignore malformed bytes and hard NUL to keep COPY/index path safe.
    line = raw.decode("utf-8", errors="ignore").replace("\x00", "").strip()
    if len(line) < 5:
        return None

    for sep in (",", "|", ";", "\t"):
        parsed = _split_first_two(line, sep)
        if parsed is not None:
            return parsed

    # Colon fallback: split from the right so URL keeps scheme/port.
    if ":" in line:
        tail_split = line.rsplit(":", 2)
        if len(tail_split) == 3:
            url, username, password = (part.strip() for part in tail_split)
            if url and username and password:
                return url, username, password

    return None


def parse_chunk_lines(payload: bytes) -> tuple[list[tuple[str, str, str]], bytes, int]:
    parsed_rows: list[tuple[str, str, str]] = []
    invalid = 0
    lines = payload.split(b"\n")
    carry = lines.pop() if lines else b""

    for raw in lines:
        parsed = parse_combo_line(raw)
        if parsed is None:
            invalid += 1
            continue
        parsed_rows.append(parsed)

    return parsed_rows, carry, invalid
