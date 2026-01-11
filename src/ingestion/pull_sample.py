import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


def main(rows: int = 5) -> None:
    # Load settings from .env
    load_dotenv()
    base_url = os.getenv("ODS_BASE_URL", "").rstrip("/")
    dataset = os.getenv("ODS_DATASET", "")

    if not base_url or not dataset:
        raise SystemExit("Missing ODS_BASE_URL or ODS_DATASET in .env")

    # Build request (Opendatasoft Search API v1)
    url = f"{base_url}/api/records/1.0/search/"
    params = {
        "dataset": dataset,
        "rows": rows,
        # keep it simple: newest modifications first
        "sort": "-last_modified_timestamp",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    # Save raw payload to Bronze
    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{dataset}__sample_{rows}__{ts}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Print tiny confirmation
    records = payload.get("records", [])
    print(f"Saved {len(records)} records to: {out_path}")

    if records:
        r0 = records[0]
        print("First record keys present:",
              "recordid" in r0,
              "fields" in r0,
              "record_timestamp" in r0)


if __name__ == "__main__":
    main()
