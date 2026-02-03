import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_bronze_records(path: Path) -> list[dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise SystemExit(f"Bronze file not found: {path}") from e

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Bronze file is not valid JSON: {path}") from e

    # Support both shapes:
    # 1) dict with "records"
    if isinstance(payload, dict):
        records = payload.get("records", [])
        if not isinstance(records, list):
            raise SystemExit('Unexpected payload: "records" is not a list.')
        return [r for r in records if isinstance(r, dict)]

    # 2) list of records
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]

    raise SystemExit("Unexpected bronze JSON shape (expected dict or list).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check duplicate recordid values in a Bronze JSON file.")
    parser.add_argument("--file", type=Path, required=True, help="Path to a Bronze JSON file.")
    parser.add_argument("--top", type=int, default=10, help="Show top N duplicate recordids (default: 10).")
    args = parser.parse_args()

    records = load_bronze_records(args.file)

    recordids = [r.get("recordid") for r in records if r.get("recordid")]
    counts = Counter(recordids)

    total = len(recordids)
    unique = len(counts)
    dupes = [(rid, c) for rid, c in counts.items() if c > 1]
    dupes_sorted = sorted(dupes, key=lambda x: x[1], reverse=True)

    print("File:", args.file)
    print("Total records with recordid:", total)
    print("Unique recordids:", unique)
    print("Duplicate recordids:", len(dupes_sorted))

    if not dupes_sorted:
        print("\nNo duplicate recordids found.")
        return

    print(f"\nTop {min(args.top, len(dupes_sorted))} duplicates:")
    for rid, c in dupes_sorted[: args.top]:
        print(f"  {rid}  ->  {c} occurrences")

    # Show timestamps for the worst offender (most duplicated)
    sample_rid, _ = dupes_sorted[0]
    ts = []
    for r in records:
        if r.get("recordid") == sample_rid:
            fields = r.get("fields", {})
            if isinstance(fields, dict):
                ts.append(fields.get("last_modified_timestamp"))

    print("\nExample duplicated recordid:", sample_rid)
    print("Its last_modified_timestamps:", ts)


if __name__ == "__main__":
    main()
