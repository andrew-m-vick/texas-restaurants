"""Ingest TABC license/permit records for the configured TARGET_CITIES.

Dataset: data.texas.gov/resource/7hf9-qc9f.json (TABC License Information).
Filters at the Socrata level to tier='Retail' — the slice that overlaps
with restaurants and bars. Re-runnable: truncates bronze and reloads.
"""
import json
from sqlalchemy import text
from ..config import TABC_LICENSES_URL, TARGET_CITIES
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

COLUMNS = [
    "master_file_id", "license_id", "license_type", "tier",
    "primary_status", "license_status", "trade_name", "owner",
    "address", "city", "state", "zip", "county",
    "original_issue_date", "current_issued_date",
    "expiration_date", "status_change_date", "gun_sign",
]


def run():
    city_list = ", ".join(f"'{c}'" for c in TARGET_CITIES)
    where = f"upper(city) in ({city_list}) AND tier='Retail'"
    with track_run("ingest_tabc_licenses", "bronze") as state, engine.begin() as conn:
        conn.execute(text("TRUNCATE bronze.licenses"))
        total = 0
        insert_sql = text(
            f"INSERT INTO bronze.licenses ({', '.join(COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in COLUMNS)}, :raw)"
        )
        for batch in paginate(TABC_LICENSES_URL, where=where, order="license_id"):
            rows = [
                {**{c: r.get(c) for c in COLUMNS}, "raw": json.dumps(r)}
                for r in batch
            ]
            conn.execute(insert_sql, rows)
            total += len(rows)
            print(f"tabc_licenses: {total} rows")
        state["rows"] = total
    print(f"done: {total} rows into bronze.licenses (cities={TARGET_CITIES})")


if __name__ == "__main__":
    run()
