import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


API_PATH = "/api/records/1.0/search/"


def utc_ts_compact() -> str:
    """Return UTC timestamp like 20260201T175332Z for filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}{API_PATH}"


def fetch_sample(url: str, dataset: str, rows: int, timeout_s: int = 30) -> dict[str, Any]:
    params = {
        "dataset": dataset,
        "rows": rows,
        # newest modifications first (helps you see recently active schema values)
        "sort": "-last_modified_timestamp",
    }

    try:
        resp = requests.get(url, params=params, timeout=timeout_s)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"HTTP request failed: {e}") from e

    try:
        payload: dict[str, Any] = resp.json()
    except ValueError as e:
        # ValueError is what requests throws for invalid JSON
        raise SystemExit("Response was not valid JSON (unexpected API response).") from e

    return payload


def save_bronze_payload(payload: dict[str, Any], dataset: str, rows: int, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{dataset}__sample_{rows}__{utc_ts_compact()}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull a small sample from City of Vancouver 3-1-1 dataset via Opendatasoft Search API v1."
    )
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to fetch (default: 5).")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/bronze"),
        help="Output directory for Bronze JSON (default: data/bronze).",
    )
    args = parser.parse_args()

    # Load settings from .env
    load_dotenv()
    base_url = os.getenv("ODS_BASE_URL", "").strip()
    dataset = os.getenv("ODS_DATASET", "").strip()

    if not base_url or not dataset:
        raise SystemExit("Missing ODS_BASE_URL or ODS_DATASET in .env")

    url = build_url(base_url)
    payload = fetch_sample(url=url, dataset=dataset, rows=args.rows)

    # Quick shape checks (this script is for schema inspection)
    top_keys = sorted(payload.keys())
    print("Top-level payload keys:", top_keys)

    records = payload.get("records", [])
    if not isinstance(records, list):
        raise SystemExit("Unexpected payload shape: 'records' is not a list.")

    out_path = save_bronze_payload(payload=payload, dataset=dataset, rows=args.rows, out_dir=args.out_dir)

    print(f"Saved {len(records)} records to: {out_path}")

    if records:
        r0 = records[0]
        if isinstance(r0, dict):
            print(
                "First record has keys:",
                "recordid" in r0,
                "fields" in r0,
                "record_timestamp" in r0,
            )
            fields = r0.get("fields", {})
            if isinstance(fields, dict):
                print("First record fields keys sample:", sorted(list(fields.keys()))[:10])
        else:
            print("First record is not a dict (unexpected).")


if __name__ == "__main__":
    main()
