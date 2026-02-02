import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .dedupe import dedupe_latest


def load_records(path: Path) -> list[dict[str, Any]]:
    """Load records from a Bronze file, supporting both shapes:
    1) list of records
    2) dict with a 'records' key (list)
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in Bronze file: {path}") from e

    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]

    if isinstance(payload, dict):
        records = payload.get("records", [])
        if not isinstance(records, list):
            raise SystemExit(f'Unexpected payload in {path}: "records" is not a list.')
        return [r for r in records if isinstance(r, dict)]

    raise SystemExit(f"Unexpected Bronze JSON shape in {path} (expected list or dict).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver: dedupe latest record per recordid across Bronze files.")
    parser.add_argument(
        "--pattern",
        type=str,
        default="*.json",
        help="Glob pattern for Bronze files (default: *.json). Example: '*__last48h__*.json'",
    )
    args = parser.parse_args()

    bronze_dir = Path("data/bronze")
    in_files = sorted(bronze_dir.glob(args.pattern))

    if not in_files:
        raise SystemExit(f"No Bronze files found in {bronze_dir} matching pattern: {args.pattern}")

    combined: list[dict[str, Any]] = []
    for f in in_files:
        recs = load_records(f)
        print(f"Loaded {len(recs)} records from {f.name}")
        combined.extend(recs)

    deduped, stats = dedupe_latest(combined)

    # Optional but helpful: deterministic order for stable diffs/tests
    deduped.sort(key=lambda r: str(r.get("recordid", "")))

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
