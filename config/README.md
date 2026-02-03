# Vancouver 3-1-1 Service Requests Pipeline (Bronze → Silver → Gold)

Portfolio data engineering project using Vancouver 3-1-1 service request data to produce **weekly trends by neighbourhood**.

## What it does
- **Bronze**: incremental ingestion from the API (raw records saved locally)
- **Silver**: deduplicate by `recordid`, keeping the latest version using `last_modified_timestamp`
- **Gold**: weekly counts:
  - by `week_start_date + local_area`
  - by `week_start_date + local_area + department`

## Data source
City of Vancouver Open Data portal (Opendatasoft Search API) dataset: `3-1-1-service-requests`

## Why ingestion uses time-window chunking
The Search API has a pagination limit (you can’t page forever in one query).
So ingestion splits the time range into smaller windows and paginates inside each window to avoid 400 errors.

## Repo structure
```
src/
  ingestion/
    pull_sample.py
    pull_recent48h.py
    check_duplicates.py
  silver/
    dedupe.py
    dedupe_latest_by_recordid.py
  gold/
    build_weekly_trends.py
config/
  README.md
  state.example.json
data/
  bronze/   (gitignored)
  silver/   (gitignored)
  gold/     (gitignored)
docs/
  ingestion.md
  silver.md
  gold.md
```

## Setup

### 1) Create venv + install deps
```bash
python -m venv .venv
source .venv/bin/activate   # Git Bash
pip install -r requirements.txt
```

### 2) Create `.env` in repo root
```env
ODS_BASE_URL=https://opendata.vancouver.ca/api/records/1.0/search/
ODS_DATASET=3-1-1-service-requests
```

### 3) Runtime state (watermark)
```bash
cp config/state.example.json config/state.json
```
Note: `config/state.json` is gitignored because it changes every run.

## Run the pipeline (prototype)

### Bronze: quick sample pull
```bash
python -m src.ingestion.pull_sample --rows 5
```

### Bronze: incremental pull (last 48h + lookback)
```bash
python -m src.ingestion.pull_recent48h
```

### Bronze: check duplicates in a Bronze file
Use forward slashes in Git Bash:
```bash
python -m src.ingestion.check_duplicates --file data/bronze/<bronze_file>.json --top 5
```

### Silver: dedupe across Bronze files
```bash
python -m src.silver.dedupe_latest_by_recordid
```

### Gold: build weekly trend CSVs
```bash
python -m src.gold.build_weekly_trends
```

## Docs
- [Ingestion (Bronze)](docs/ingestion.md)
- [Silver (Deduped)](docs/silver.md)
- [Gold (Weekly Trends)](docs/gold.md)



## Outputs
Generated outputs are written under `data/` and are gitignored:
- `data/bronze/` raw API payloads
- `data/silver/` deduped JSON
- `data/gold/` weekly trend CSVs

## CI
GitHub Actions runs:
- Ruff linting
- `compileall` to catch syntax/import errors

## Next upgrades (post-prototype)
- Unit tests (dedupe + weekly aggregation)
- Backfill for historical ranges
- Orchestration + scheduling
- Cloud deployment (S3 + orchestration + monitoring)
