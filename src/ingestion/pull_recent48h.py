import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


API_PATH = "/api/records/1.0/search/"
STATE_PATH = Path("config/state.json")
BRONZE_DIR = Path("data/bronze")
CHUNK_SIZE_HOURS = 6


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def normalize_iso_ts(s: str) -> str:
    """
    Normalize common API timestamp variants for datetime.fromisoformat().
    - Converts trailing 'Z' to '+00:00'
    """
    s = s.strip()
    if s.endswith("Z"):
        return s[:-1] + "+00:00"
    return s


def parse_iso_dt(s: str) -> datetime:
    """
    Parse ISO datetime string into timezone-aware datetime (UTC).
    Returns DT_MIN on failure.
    """
    try:
        dt = datetime.fromisoformat(normalize_iso_ts(s))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

    if dt.tzinfo is None:
        # If upstream ever gives a naive timestamp, assume UTC
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        return {"last_watermark": None}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        # If state got corrupted, fail loudly with a helpful message
        raise SystemExit(f"State file is not valid JSON: {path}")

    if not isinstance(data, dict):
        raise SystemExit(f"State file must contain a JSON object: {path}")

    data.setdefault("last_watermark", None)
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def build_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}{API_PATH}"


def fetch_page(
    url: str,
    dataset: str,
    chunk_start_iso: str,
    chunk_end_iso: str,
    start: int,
    page_size: int,
    timeout_s: int,
) -> dict[str, Any]:
    q = f'last_modified_timestamp >= "{chunk_start_iso}" AND last_modified_timestamp < "{chunk_end_iso}"'
    params = {
        "dataset": dataset,
        "q": q,
        "rows": page_size,
        "start": start,
        "sort": "-last_modified_timestamp",
    }

    try:
        resp = requests.get(url, params=params, timeout=timeout_s)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"HTTP request failed: {e}") from e

    try:
        payload = resp.json()
    except ValueError as e:
        raise SystemExit("API response was not valid JSON.") from e

    if not isinstance(payload, dict):
        raise SystemExit("Unexpected API payload shape (expected JSON object).")

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally pull recent 3-1-1 records (last_modified_timestamp) into Bronze using a watermark + lookback."
    )
    parser.add_argument("--hours", type=int, default=48, help="Fallback window for first run (default: 48).")
    parser.add_argument("--lookback-hours", type=int, default=24, help="Safety lookback to catch late updates (default: 24).")
    parser.add_argument("--page-size", type=int, default=1000, help="Rows per API page (default: 1000). Max is typically 1000.")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds (default: 30).")
    args = parser.parse_args()

    load_dotenv()
    base_url = os.getenv("ODS_BASE_URL", "").strip()
    dataset = os.getenv("ODS_DATASET", "").strip()

    if not base_url or not dataset:
        raise SystemExit("Missing ODS_BASE_URL or ODS_DATASET in .env")

    now = utc_now()
    fallback_start = now - timedelta(hours=args.hours)

    state = load_state(STATE_PATH)
    last_watermark = state.get("last_watermark")

    if last_watermark:
        lw_dt = parse_iso_dt(str(last_watermark))
        if lw_dt == datetime.min.replace(tzinfo=timezone.utc):
            raise SystemExit(f"Invalid last_watermark in state.json: {last_watermark}")
        effective_start = lw_dt - timedelta(hours=args.lookback_hours)
    else:
        effective_start = fallback_start

    effective_start_iso = effective_start.replace(microsecond=0).isoformat()

    effective_end = effective_start + timedelta(hours=CHUNK_SIZE_HOURS)
    effective_end_iso = effective_end.replace(microsecond=0).isoformat()

    print("Last watermark:", last_watermark)
    print("Effective start:", effective_start_iso)

    url = build_url(base_url)

    all_records: list[dict[str, Any]] = []
    start = 0
    page = 0
   
    range_start_dt = effective_start
    range_end_dt = now
    chunk_start_dt = range_start_dt
    counter = 1

    while chunk_start_dt <= range_end_dt:

        chunk_end_dt = min(chunk_start_dt + timedelta(hours=CHUNK_SIZE_HOURS), now)
        if chunk_end_dt >= range_end_dt:
            break
        chunk_start_iso = chunk_start_dt.replace(microsecond=0).isoformat()
        chunk_end_iso = chunk_end_dt.replace(microsecond=0).isoformat()
        print(f"\nFetching chunk: {chunk_start_iso} to {chunk_end_iso}, run {counter}")
        counter += 1

        # chunk_records: list[dict[str, Any]] = []
        # start = 0

        # #Fetch pages within the chunk
        # while True:
        #     payload = fetch_page(
        #         url=url,
        #         dataset=dataset,
        #         chunk_start_iso=chunk_start_iso,
        #         chunk_end_iso=chunk_end_iso,
        #         start=start,
        #         page_size=args.page_size,
        #         timeout_s=args.timeout,
        #     )

        #     records = payload.get("records", [])
        #     if not isinstance(records, list):
        #         raise SystemExit("Unexpected payload: 'records' is not a list.")

        #     nhits = payload.get("nhits", 0)
        #     if not isinstance(nhits, int):
        #         nhits = 0

        #     chunk_records.extend(records)
        #     chunk_page += 1
        #     print(f"  Page {chunk_page}: pulled {len(records)} records. Chunk total: {len(chunk_records)} / {nhits}")

        #     chunk_start += args.page_size
        #     if not records or (nhits and len(chunk_records) >= nhits):
        #         break

        # all_records.extend(chunk_records)
        chunk_start_dt = chunk_end_dt

        # Move to next chunk
    #     effective_start = effective_end
    #     effective_start_iso = effective_end_iso
    #     effective_end_iso = effective_start_iso + timedelta(hours=CHUNK_SIZE_HOURS)
    #     start = 0  # Reset for next chunk
    # BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    # out_path = BRONZE_DIR / f"{dataset}__last{args.hours}h__{utc_ts_compact(now)}.json"
    # out_path.write_text(json.dumps(all_records, indent=2), encoding="utf-8")
    # print(f"Saved {len(all_records)} records to: {out_path}")

    # # Update watermark based on max last_modified_timestamp present
    # timestamps = []
    # for r in all_records:
    #     fields = r.get("fields", {})
    #     if isinstance(fields, dict):
    #         ts = fields.get("last_modified_timestamp")
    #         if ts:
    #             timestamps.append(str(ts))

    # if not timestamps:
    #     print("No last_modified_timestamp found. State not updated.")
    #     return

    # max_dt = max((parse_iso_dt(t) for t in timestamps))
    # if max_dt == datetime.min.replace(tzinfo=timezone.utc):
    #     print("Could not parse any last_modified_timestamp. State not updated.")
    #     return

    # new_watermark = max_dt.replace(microsecond=0).isoformat()
    # state["last_watermark"] = new_watermark
    # save_state(STATE_PATH, state)
    # print("Updated last_watermark to:", new_watermark)


if __name__ == "__main__":
    main()  
