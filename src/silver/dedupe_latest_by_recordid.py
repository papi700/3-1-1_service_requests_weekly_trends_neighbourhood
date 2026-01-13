# src/silver/dedupe_latest_by_recordid.py

import json
from datetime import datetime, timezone
from pathlib import Path

from .dedupe import dedupe_latest


def load_records(path: Path) -> list[dict]:
    """Load records from a Bronze file, supporting both shapes:
    1) list of records
    2) dict with a 'records' key
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    return payload.get("records", [])


def main() -> None:
    bronze_dir = Path("data/bronze")
    in_files = sorted(bronze_dir.glob("*.json"))

    if not in_files:
        raise SystemExit("No Bronze files found in data/bronze")

    combined: list[dict] = []
    for f in in_files:
        recs = load_records(f)
        print(f"Loaded {len(recs)} records from {f.name}")
        combined.extend(recs)

    deduped, stats = dedupe_latest(combined)

    print("\n--- Dedupe stats ---")
    print("Input records (combined):", stats["input_records"])
    print("Unique recordids kept:", stats["kept_records"])
    print("Duplicates dropped:", stats["input_records"] - stats["kept_records"])
    print("Missing recordid skipped:", stats["missing_id"])
    print("Invalid/missing timestamps seen:", stats["invalid_or_missing_ts"])

    out_dir = Path("data/silver")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"311_requests__silver_deduped__{run_ts}.json"
    out_path.write_text(json.dumps(deduped, indent=2), encoding="utf-8")
    print("\nSaved Silver deduped file to:", out_path)


if __name__ == "__main__":
    main()
