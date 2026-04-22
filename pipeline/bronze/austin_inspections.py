"""Ingest City of Austin food establishment inspection scores.

Dataset: data.austintexas.gov/resource/ecmv-9xxi.json
Writes to bronze.inspections with city='AUSTIN'. Austin does not publish
separate violation detail.
"""
import json
from sqlalchemy import text
from ..config import AUSTIN_INSPECTIONS_URL
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

COLUMNS = [
    "city", "facility_id", "restaurant_name", "address", "zip_code",
    "inspection_date", "score", "inspection_type", "latitude", "longitude",
]


def run():
    with track_run("ingest_austin_inspections", "bronze") as state, engine.begin() as conn:
        conn.execute(text("TRUNCATE bronze.inspections"))
        total = 0
        insert_sql = text(
            f"INSERT INTO bronze.inspections ({', '.join(COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in COLUMNS)}, :raw)"
        )
        for batch in paginate(AUSTIN_INSPECTIONS_URL, order="inspection_date"):
            rows = []
            for r in batch:
                rows.append({
                    "city": "AUSTIN",
                    "facility_id": r.get("facility_id"),
                    "restaurant_name": r.get("restaurant_name"),
                    "address": r.get("address"),
                    "zip_code": r.get("zip_code"),
                    "inspection_date": r.get("inspection_date"),
                    "score": r.get("score"),
                    "inspection_type": r.get("process_description"),
                    "latitude": None,
                    "longitude": None,
                    "raw": json.dumps(r),
                })
            conn.execute(insert_sql, rows)
            total += len(rows)
            print(f"austin_inspections: {total} rows")
        state["rows"] = total
    print(f"done: {total} rows into bronze.inspections (AUSTIN)")


if __name__ == "__main__":
    run()
