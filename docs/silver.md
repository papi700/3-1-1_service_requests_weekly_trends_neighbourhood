# Silver (Deduped) — Latest Record per `recordid`

The Silver layer turns raw Bronze API records into a **clean, record-level dataset** by deduplicating on `recordid`.

Why this exists:
- The API can return **multiple versions** of the same service request across different pulls.
- We want **one row per service request**, keeping the **latest version**.

## Inputs
- One or more JSON files from `data/bronze/` (raw API responses saved by ingestion).

## Outputs
- A single timestamped JSON file written to `data/silver/`:
  - `311_requests__silver_deduped__<timestamp>.json`

This output is gitignored (generated data).

## Scripts

### `dedupe.py`
Purpose: reusable deduplication helper(s).

Core rule:
- Group records by `recordid`
- Keep the record with the greatest (latest) `last_modified_timestamp`

Edge cases handled:
- Missing `recordid`: cannot dedupe -> typically **skipped** (and counted)
- Missing/invalid `last_modified_timestamp`: record cannot be reliably ordered -> typically **skipped** or treated as lowest priority (depending on implementation)

This file is intended to be unit-testable.

### `dedupe_latest_by_recordid.py`
Purpose: end-to-end Silver build from Bronze files.

What it does:
1. Reads multiple Bronze JSON files
2. Combines all records
3. Dedupes by `recordid` keeping the latest `last_modified_timestamp`
4. Writes one deduped Silver file to `data/silver/`
5. Prints summary stats (inputs, uniques kept, duplicates dropped, etc.)

Run:
```bash
python -m src.silver.dedupe_latest_by_recordid
```

## Diagnostics
If you want to verify why Silver is necessary, run the Bronze duplicate checker first:

```bash
python -m src.ingestion.check_duplicates --file data/bronze/<bronze_file>.json --top 5
```

## Data quality notes
- Deduplication is only as good as `last_modified_timestamp`.
- If timestamps are missing or invalid, those records should be tracked in stats so you can see the “data gap” instead of silently losing data.

## Failure modes to know
- **Import errors**: usually means `src/` packages aren’t treated as modules; run scripts with `python -m ...`
- **Mixed schemas across files**: older Bronze files might differ slightly; the dedupe logic should be resilient to missing keys.
