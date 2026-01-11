import json
from collections import Counter
from pathlib import Path

# 1) Point this to your latest Bronze JSON file
BRONZE_FILE = Path("data/bronze/3-1-1-service-requests__last48h__20260108T004232Z.json")

payload = json.loads(BRONZE_FILE.read_text(encoding="utf-8"))
records = payload.get("records", [])

recordids = [r.get("recordid") for r in records if r.get("recordid")]
counts = Counter(recordids)

total = len(recordids)
unique = len(counts)
dupes = [rid for rid, c in counts.items() if c > 1]

print("Total records:", total)
print("Unique recordids:", unique)
print("Duplicate recordids:", len(dupes))

if dupes:
    sample_rid = dupes[0]
    ts = []
    for r in records:
        if r.get("recordid") == sample_rid:
            ts.append(r.get("fields", {}).get("last_modified_timestamp"))
    print("\nExample duplicated recordid:", sample_rid)
    print("Its last_modified_timestamps:", ts)
else:
    print("\nNo duplicate recordids found in this file.")
