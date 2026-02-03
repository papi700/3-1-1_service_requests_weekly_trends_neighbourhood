# Gold (Analytics) — Weekly Trends by Neighbourhood

The Gold layer produces **analytics-ready weekly trend tables** for reporting and dashboarding.

Goal:
- “Weekly trends of 3-1-1 request types per neighbourhood”

In the prototype, we output **two CSVs**:
1. Weekly counts by neighbourhood (`local_area`)
2. Weekly counts by neighbourhood + department (`department`)

## Inputs
- A deduped Silver JSON file from `data/silver/`:
  - `311_requests__silver_deduped__<timestamp>.json`

## Outputs
Two timestamped CSVs in `data/gold/`:
- `311_requests__gold_weekly_by_local_area__<timestamp>.csv`
- `311_requests__gold_weekly_by_local_area_and_department__<timestamp>.csv`

These outputs are gitignored (generated data).

## Script

### `build_weekly_trends.py`
Purpose: transform Silver records into weekly aggregates.

What it does (high level):
1. Load the Silver file (list of API record objects)
2. For each record:
   - Extract `service_request_open_timestamp`
   - Convert it into a **week start date** (Monday, prototype standard)
   - Extract:
     - `local_area` (neighbourhood)
     - `department` (request owner / org unit)
   - Fill missing values with `UNKNOWN` (and track stats)
3. Aggregate counts using group keys:
   - (week_start_date, local_area)
   - (week_start_date, local_area, department)
4. Write both aggregations to CSV using `csv.DictWriter`
5. Print validation checks (example: sums of request_count match input count)

Run:
```bash
python -m src.gold.build_weekly_trends
```

## Output schemas

### Weekly by neighbourhood
Columns:
- `week_start_date` (YYYY-MM-DD)
- `local_area`
- `request_count`

Example row:
- `2026-01-05, Kitsilano, 66`

### Weekly by neighbourhood + department
Columns:
- `week_start_date` (YYYY-MM-DD)
- `local_area`
- `department`
- `request_count`

Example row:
- `2026-01-05, Kitsilano, ENG - Sanitation Services, 12`

## Sorting (why it’s not “by request_count”)
The CSVs are primarily used as **time series tables**:
- Sorting by week (then area, then department) makes it easy to:
  - scan trends week-over-week
  - diff outputs across runs
  - load into BI tools consistently

If you want “top request counts”, that’s usually a **separate derived view** (e.g., top N areas for a specific week).

## Data quality notes
- Some records may be missing `local_area` or `department` keys.
- In prototype v1, missing/empty values are treated as `UNKNOWN` and counted in stats.
- This makes data gaps visible instead of silently dropping records.

## Failure modes to know
- **Unexpected missing keys**: the script should use safe access (`dict.get`) and track counts.
- **Timestamp parsing**: invalid or missing timestamps should be tracked and the record skipped (or coerced) depending on the design.
- **Week logic**: changing “week start day” changes aggregation; keep it consistent and documented.
