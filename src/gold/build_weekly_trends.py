import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from operator import itemgetter
import csv

DT_MIN = datetime.min.replace(tzinfo=timezone.utc)


def get_latest_silver_file(silver_dir: str | Path) -> str | None:
    """
    Safely retrieves the latest JSON file from the silver directory 
    based on the timestamp in the filename.
    """
    path = Path(silver_dir)
    
    # 1. Gather files (Returns an empty list if none found)
    files = list(path.glob("311_requests__silver_deduped_*.json"))
    
    # 2. Guard Clause: Handle empty directory safely
    if not files:
        return None
        
    # 3. Pick the latest
    # Since filenames are ISO formatted, alphabetical max == chronological latest.
    return max(files)

def load_records(path: Path) -> list[dict]:
    """Load records from a Silver file, supporting both shapes:
    1) list of records
    2) dict with a 'records' key
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    return payload.get("records", [])

def _to_dt(s: str | None) -> datetime:
    """Parse ISO timestamp; return DT_MIN if missing/invalid."""
    if not s:
        return DT_MIN
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return DT_MIN
    
def get_field(field_name: str, fields: dict) -> dict[str, str | bool]:
    if field_name in fields:
        value = fields.get(field_name)
        if value and value.strip() != "":
            return {"value": value.strip(), "missing_key": False, "empty_value": False}
        else:
            return {"value": "UNKNOWN", "missing_key": False, "empty_value": True}
    else:
        return {"value": "UNKNOWN", "missing_key": True, "empty_value": False}

def main() -> None:
    silver_dir = Path("data/silver")    
    path = get_latest_silver_file(silver_dir)

    if not path:
        raise SystemExit("No silver files found in data/silver")
    
    payload = load_records(path)
    if not isinstance(payload, list):
        raise SystemExit("Not a list")
    if len(payload) == 0:
        raise SystemExit("No records found in silver file")
    
    stats = {
        "input_records": 0,
        "produced_rows": 0,
        "invalid_or_missing_ts": 0,
        "unknown_local_area_count": 0,
        "unknown_department_count": 0,
        "unknown_any_count": 0,
        "unknown_both_count": 0,
        "missing_fields_local_area": 0,
        "missing_fields_department": 0,
        "empty_local_area_value": 0,
        "empty_department_value": 0,
        "min_week_start_date": "",
        "max_week_start_date": "",
        "sum_of_request_count_in_week_and_area": 0,
        "sum_of_request_count_in_week_area_and_dept": 0,
        "week_and_area_csv_output_path": "",
        "week_area_and_dept_csv_output_path": "",
        "week_and_area_row_csv_count": 0,
        "week_area_and_dept_row_csv_count": 0
    }
    stats["input_records"] = len(payload)

    rows: list[dict] = []
    
    sample_missing_local_area_key : dict = {}
    sample_missing_department_key : dict = {}
    sample_empty_local_area_value : dict = {}
    sample_empty_department_value : dict = {}
    for r in payload:
        fields = r.get("fields", {})
        dt = _to_dt(fields.get("service_request_open_timestamp"))
        if dt == DT_MIN:
            stats["invalid_or_missing_ts"] += 1
            continue
        else:
            week_day = dt.weekday()
            raw_week_start_date = dt - timedelta(days=week_day)
            clean_week_start_date = raw_week_start_date.date().strftime("%Y-%m-%d")
            stats["min_week_start_date"] = clean_week_start_date if stats["min_week_start_date"] == "" or clean_week_start_date < stats["min_week_start_date"] else stats["min_week_start_date"]
            stats["max_week_start_date"] = clean_week_start_date if stats["max_week_start_date"] == "" or clean_week_start_date > stats["max_week_start_date"] else stats["max_week_start_date"]
     
        local_area_info = get_field("local_area", fields)
        local_area = local_area_info.get("value")
        if local_area == "UNKNOWN":
            stats["unknown_local_area_count"] += 1
            if local_area_info.get("missing_key"):
                stats["missing_fields_local_area"] += 1
                sample_missing_local_area_key = r if not sample_missing_local_area_key else sample_missing_local_area_key
            elif local_area_info.get("empty_value"):
                stats["empty_local_area_value"] += 1
                sample_empty_local_area_value = r if not sample_empty_local_area_value else sample_empty_local_area_value
        
        department_info = get_field("department", fields)
        department = department_info.get("value")
        if department == "UNKNOWN":
            stats["unknown_department_count"] += 1
            if department_info.get("missing_key"):
                stats["missing_fields_department"] += 1
                sample_missing_department_key = r if not sample_missing_department_key else sample_missing_department_key
            elif department_info.get("empty_value"):
                stats["empty_department_value"] += 1
                sample_empty_department_value = r if not sample_empty_department_value else sample_empty_department_value   
       
        row = {
            "week_start_date": clean_week_start_date,
            "local_area": local_area,
            "department": department
        }
        rows.append(row)

        stats["produced_rows"] += 1
        if department == "UNKNOWN" or local_area == "UNKNOWN":
            stats["unknown_any_count"] += 1
        if department == "UNKNOWN" and local_area == "UNKNOWN":
            stats["unknown_both_count"] += 1

    def sample_to_json(sample: dict) -> str:
        if not sample:
            return "None found"
        return json.dumps(sample, indent=2)
    
    week_and_area_counts: dict[tuple[str, str], int] = {}
    week_area_and_dept_counts: dict[tuple[str, str, str], int] = {}
    for r in rows:
        week_and_area = (r["week_start_date"], r["local_area"])
        week_and_area_counts[week_and_area] = week_and_area_counts[week_and_area] + 1 if week_and_area in week_and_area_counts else 1 

        week_area_and_dept = (r["week_start_date"], r["local_area"], r["department"])
        week_area_and_dept_counts[week_area_and_dept] = week_area_and_dept_counts[week_area_and_dept] + 1 if week_area_and_dept in week_area_and_dept_counts else 1

    
    week_and_area_rows: list[dict] = []
    for (week_start_date, local_area), count in week_and_area_counts.items():
        week_and_area_rows.append({
            "week_start_date": week_start_date,
            "local_area": local_area,
            "request_count": count
        })
    week_and_area_rows.sort(key=itemgetter("week_start_date", "local_area"))

    week_area_and_dept_rows: list[dict] = []
    for (week_start_date, local_area, department), count in week_area_and_dept_counts.items():
        week_area_and_dept_rows.append({
            "week_start_date": week_start_date,
            "local_area": local_area,
            "department": department,
            "request_count": count
        })
    week_area_and_dept_rows.sort(key=itemgetter("week_start_date", "local_area", "department"))

    stats["sum_of_request_count_in_week_and_area"] = sum(r["request_count"] for r in week_and_area_rows)
    stats["sum_of_request_count_in_week_area_and_dept"] = sum(r["request_count"] for r in week_area_and_dept_rows)

    out_dir = Path("data/gold")
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path_week_area = out_dir / f"311_requests__gold_weekly_by_local_area__{run_ts}.csv"
    out_path_week_area_dept = out_dir / f"311_requests__gold_weekly_by_local_area_and_department__{run_ts}.csv"
    stats["week_and_area_csv_output_path"] = str(out_path_week_area)
    stats["week_area_and_dept_csv_output_path"] = str(out_path_week_area_dept)

    with open(out_path_week_area, "w", encoding="utf-8", newline="") as f:
        if  not week_and_area_rows:
            raise SystemExit("No rows to write for week and area CSV")
        fieldnames = week_and_area_rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(week_and_area_rows)

    with open(out_path_week_area_dept, "w", encoding="utf-8", newline="") as f:
        if not week_area_and_dept_rows:
            raise SystemExit("No rows to write for week, area and dept CSV")
        fieldnames = week_area_and_dept_rows[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(week_area_and_dept_rows)

    def row_count_csv(path: Path) -> int:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            return sum(1 for _ in reader)

    stats["week_and_area_row_csv_count"] = row_count_csv(out_path_week_area)
    stats["week_area_and_dept_row_csv_count"] = row_count_csv(out_path_week_area_dept)

    

    print(f"Loaded Silver file:{path}")
    print(f"Records:{len(payload)}")
    print("\n---Gold Weekly Stats---")
    print("Input records:", stats["input_records"])
    print("Produced rows:", stats["produced_rows"])
    print("Skipped rows due to invalid or missing ts:", stats["invalid_or_missing_ts"])
    print("Unknown local area:", stats["unknown_local_area_count"])
    print("Unknown department:", stats["unknown_department_count"])
    print("Unknown any (local area or department):", stats["unknown_any_count"])
    print("Unknown both (local area and department):", stats["unknown_both_count"])
    print("Missing fields - local area:", stats["missing_fields_local_area"])
    print("Missing fields - department:", stats["missing_fields_department"])
    print("Empty local area value:", stats["empty_local_area_value"])
    print("Empty department value:", stats["empty_department_value"])
    print("Min week start date:", stats["min_week_start_date"])
    print("Max week start date:", stats["max_week_start_date"])
    print("\n---Sample Records---")
    print("Sample missing local area key:", sample_to_json(sample_missing_local_area_key))
    print("Sample missing department key:", sample_to_json(sample_missing_department_key))
    print("Sample empty local area value:", sample_to_json(sample_empty_local_area_value))
    print("Sample empty department value:", sample_to_json(sample_empty_department_value))
    print("\n---Request Count Verification---")
    print("Sum of request_count in week and area:", stats["sum_of_request_count_in_week_and_area"])
    print("Sum of request_count in week, area and dept:", stats["sum_of_request_count_in_week_area_and_dept"])
    print("\n---Output CSVs---")
    print(f"Week and Area CSV Path: {stats['week_and_area_csv_output_path']}, Rows: {stats['week_and_area_row_csv_count']}")
    print(f"Week, Area and Dept CSV Path: {stats['week_area_and_dept_csv_output_path']}, Rows: {stats['week_area_and_dept_row_csv_count']}")    

if __name__ == "__main__":
    main()
