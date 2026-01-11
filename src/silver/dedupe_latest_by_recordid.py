# src/silver/dedupe_latest_by_recordid.py

import json
from datetime import datetime, timezone
from pathlib import Path


def load_records(path: Path) -> list[dict]:
    """Load records from a Bronze file, supporting both shapes:
    1) list of records
    2) dict with a 'records' key
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    return payload.get("records", [])


def ts(s: str) -> datetime:
    """Parse ISO timestamp like '2026-01-07T05:52:27+00:00'."""
    return datetime.fromisoformat(s)


def main() -> None:
    bronze_dir = Path("data/bronze")
    in_files = [
        bronze_dir / "3-1-1-service-requests__last48h__20260108T041107Z.json",
        bronze_dir / "3-1-1-service-requests__last48h__20260108T065048Z.json",
    ]

    # 1) Load and combine
    combined = []
    for f in in_files:
        recs = load_records(f)
        print(f"Loaded {len(recs)} records from {f.name}")
        combined.extend(recs)

    # 2) Dedupe by recordid, keep latest last_modified_timestamp
    best_by_id: dict[str, dict] = {}
    missing_id = 0
    missing_ts = 0

    for r in combined:
        rid = r.get("recordid")
        if not rid:
            missing_id += 1
            continue

        lm = r.get("fields", {}).get("last_modified_timestamp")
        if not lm:
            missing_ts += 1
            continue

        if rid not in best_by_id:
            best_by_id[rid] = r
        else:
            current_lm = best_by_id[rid]["fields"]["last_modified_timestamp"]
            if ts(lm) > ts(current_lm):
                best_by_id[rid] = r

    deduped = list(best_by_id.values())

    # 3) Report stats
    print("\n--- Dedupe stats ---")
    print("Input records (combined):", len(combined))
    print("Unique recordids kept:", len(deduped))
    print("Duplicates dropped:", len(combined) - len(deduped))
    print("Missing recordid skipped:", missing_id)
    print("Missing last_modified_timestamp skipped:", missing_ts)

    # 4) Save Silver output
    out_dir = Path("data/silver")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"311_requests__silver_deduped__{run_ts}.json"
    out_path.write_text(json.dumps(deduped, indent=2), encoding="utf-8")
    print("\nSaved Silver deduped file to:", out_path)


if __name__ == "__main__":
    main()
