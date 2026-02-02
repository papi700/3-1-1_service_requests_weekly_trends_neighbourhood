from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc
DT_MIN = datetime.min.replace(tzinfo=UTC)


def _to_dt(s: str | None) -> datetime:
    """
    Parse an ISO-ish timestamp string into a timezone-aware UTC datetime.

    Handles common API formats:
    - "2026-01-06T19:50:58+00:00"
    - "2026-01-07T14:49:52.065Z"  (Z means UTC)

    Returns DT_MIN if missing/invalid.
    """
    if not s:
        return DT_MIN

    s = s.strip()
    if s.endswith("Z"):
        # Convert "Z" (UTC) into "+00:00" so fromisoformat() can parse it reliably.
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return DT_MIN

    # Ensure tz-aware; if upstream gives naive timestamps, assume UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(UTC)


def dedupe_latest(
    records: list[dict[str, Any]],
    id_key: str = "recordid",
    ts_key: str = "last_modified_timestamp",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Keep the latest version per recordid based on fields[ts_key].

    Returns: (deduped_records, stats)
    """
    best_by_id: dict[str, dict[str, Any]] = {}
    stats = {
        "input_records": len(records),
        "kept_records": 0,
        "missing_id": 0,
        "invalid_or_missing_ts": 0,
    }

    for r in records:
        rid = r.get(id_key)
        if not rid:
            stats["missing_id"] += 1
            continue

        fields = r.get("fields") or {}
        lm_raw = fields.get(ts_key)
        lm_dt = _to_dt(lm_raw)

        if lm_dt == DT_MIN:
            stats["invalid_or_missing_ts"] += 1

        prev = best_by_id.get(rid)
        if prev is None:
            best_by_id[rid] = r
            continue

        prev_fields = prev.get("fields") or {}
        prev_dt = _to_dt(prev_fields.get(ts_key))

        if lm_dt > prev_dt:
            best_by_id[rid] = r

    deduped = list(best_by_id.values())
    stats["kept_records"] = len(deduped)
    return deduped, stats
