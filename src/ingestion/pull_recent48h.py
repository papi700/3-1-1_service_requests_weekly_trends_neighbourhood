# src/ingestion/pull_recent_48h.py

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


def main(hours: int = 48, rows: int = 1000) -> None:
    load_dotenv()
    base_url = os.getenv("ODS_BASE_URL", "").rstrip("/")
    dataset = os.getenv("ODS_DATASET", "")
    if not base_url or not dataset:
        raise SystemExit("Missing ODS_BASE_URL or ODS_DATASET in .env")

    NOW = datetime.now(timezone.utc)
    START = NOW - timedelta(hours=hours)
    START_ISO = START.replace(microsecond=0).isoformat()

    STATE_PATH = Path("config/state.json")
    LOOKBACK_HOURS = 24  # safety buffer to catch late updates

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    last_watermark = state.get("last_watermark")  # either an ISO timestamp string, or None

    if last_watermark is None:
        # First run: use the 48h start you already computed
        EFFECTIVE_START_ISO = START_ISO
    else:
        # Later runs: pull from (last watermark - lookback)
        lw = datetime.fromisoformat(last_watermark)  # uses the +00:00 offset in the string
        effective_start = lw - timedelta(hours=LOOKBACK_HOURS)
        EFFECTIVE_START_ISO = effective_start.replace(microsecond=0).isoformat()

    print("Last watermark:", last_watermark)
    print("Effective start:", EFFECTIVE_START_ISO)

    q = f'last_modified_timestamp >= "{EFFECTIVE_START_ISO}"'

    URL = f"{base_url}/api/records/1.0/search/"
    PARAMS = {
        "dataset": dataset,
        "q": q,
        "rows": rows,
        "sort": "-last_modified_timestamp",
    }

    ALL_RECORDS = []
    ROWS_PER_PAGE = 1000
    start = 0

    while True:
        PARAMS["rows"] = ROWS_PER_PAGE
        PARAMS["start"] = start

        RESP = requests.get(URL, params=PARAMS, timeout=30)
        RESP.raise_for_status()
        PAYLOAD = RESP.json()

        records = PAYLOAD.get("records", [])
        ALL_RECORDS.extend(records)

        nhits = PAYLOAD.get("nhits", 0)
        print(f"Pulled {len(records)} records this page. Total so far: {len(ALL_RECORDS)} / {nhits}")

        start += ROWS_PER_PAGE
        if len(ALL_RECORDS) >= nhits or not records:
            break
        
    
    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = NOW.strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{dataset}__last{hours}h__{ts}.json"
    out_path.write_text(json.dumps(ALL_RECORDS, indent=2), encoding="utf-8")
    print(f"Saved {len(ALL_RECORDS)} records to: {out_path}")

    timestamps = [
        r.get("fields", {}).get("last_modified_timestamp")
        for r in ALL_RECORDS
        if r.get("fields", {}).get("last_modified_timestamp")
    ]

    if not timestamps:
        print("No last_modified_timestamp found. State not updated.")
    else:
        # Parse ISO timestamps like "2026-01-05T06:36:10+00:00"
        def to_dt(s: str) -> datetime:
            return datetime.fromisoformat(s)

        new_watermark = max(timestamps, key=to_dt)

        state_path = Path("config/state.json")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["last_watermark"] = new_watermark
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

        print("Updated last_watermark to:", new_watermark)


if __name__ == "__main__":
    main()
    

