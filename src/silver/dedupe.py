# src/silver/dedupe.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


DT_MIN = datetime.min.replace(tzinfo=timezone.utc)


def _to_dt(s: str | None) -> datetime:
    """Parse ISO timestamp; return DT_MIN if missing/invalid."""
    if not s:
        return DT_MIN
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return DT_MIN


def dedupe_latest(
    records: list[dict[str, Any]],
    id_key: str = "recordid",
    ts_key: str = "last_modified_timestamp",
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Keep the latest version per recordid based on fields[last_modified_timestamp].

    Returns: (deduped_records, stats)
    """
    best_by_id: dict[str, dict[str, Any]] = {}
    stats = {
        "input_records": 0,
        "kept_records": 0,
        "missing_id": 0,
        "invalid_or_missing_ts": 0,
    }

    stats["input_records"] = len(records)

    for r in records:
        rid = r.get(id_key)
        if not rid:
            stats["missing_id"] += 1
            continue

        lm_raw = (r.get("fields") or {}).get(ts_key)
        lm_dt = _to_dt(lm_raw)
        if lm_dt is DT_MIN:
            stats["invalid_or_missing_ts"] += 1

        prev = best_by_id.get(rid)
        if prev is None:
            best_by_id[rid] = r
            continue

        prev_raw = (prev.get("fields") or {}).get(ts_key)
        prev_dt = _to_dt(prev_raw)

        if lm_dt > prev_dt:
            best_by_id[rid] = r

    deduped = list(best_by_id.values())
    stats["kept_records"] = len(deduped)
    return deduped, stats
