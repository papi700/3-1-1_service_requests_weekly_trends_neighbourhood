# Ingestion (Bronze) — Vancouver 3-1-1 Service Requests

This module pulls raw records from the Vancouver Open Data API (Opendatasoft Search API) and writes timestamped JSON files to `data/bronze/`.

## Scripts

### `pull_sample.py`
Purpose: quick validation of API shape and fields.

What it does:
- Pulls a small number of rows (default: 5)
- Writes the raw response to `data/bronze/` with a timestamped filename

Run:
```bash
python -m src.ingestion.pull_sample --rows 5
```

### `pull_recent48h.py`
Purpose: incremental ingestion for the last 48 hours, using a watermark + lookback window to catch late updates.

Key ideas (plain English):
- **Watermark**: the most recent `last_modified_timestamp` seen in the last successful run
- **Lookback**: subtract time from the watermark so you re-fetch a small overlap window to catch late updates
- **Chunking**: the API can’t paginate infinitely deep, so the script splits the time range into smaller time windows and paginates inside each window

What it does:
- Loads watermark from `config/state.json` (runtime file, gitignored)
- Computes `effective_start = last_watermark - lookback`
- Pulls records where `last_modified_timestamp` is within each time chunk
- Paginates within the chunk (page size usually 1000)
- Writes a timestamped Bronze file to `data/bronze/`
- Updates watermark only after a successful run (to the newest timestamp seen)

Run:
```bash
python -m src.ingestion.pull_recent48h
```

## Diagnostics

### `check_duplicates.py`
Purpose: confirm whether a Bronze file contains multiple versions of the same record.

What it does:
- Reads a Bronze JSON file
- Counts duplicate `recordid` values
- Prints the top duplicates and examples

Run (Git Bash path style):
```bash
python -m src.ingestion.check_duplicates --file data/bronze/<bronze_file>.json --top 5
```

## Config / State

### `.env`
Required in repo root:
- `ODS_BASE_URL`
- `ODS_DATASET`

### `config/state.json`
Runtime state file holding the watermark.
- This file is gitignored (it changes every run).
- Use `config/state.example.json` as the template.

Create runtime state:
```bash
cp config/state.example.json config/state.json
```

## Failure modes to know
- If a chunk still returns too many results, reduce chunk size (smaller time windows).
- If the API returns 400s, verify query formatting and ensure pagination isn’t exceeding limits.
