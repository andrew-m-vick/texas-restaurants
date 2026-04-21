"""Ingest City of Austin food establishment inspection scores.

Dataset: data.austintexas.gov/resource/ecmv-9xxi.json
Schema: restaurant_name, zip_code, inspection_date, score, address,
        facility_id, process_description

Austin publishes only inspection scores — no separate violations dataset.
"""
import json
from sqlalchemy import text
from ..config import AUSTIN_INSPECTIONS_URL
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

COLUMNS = [
    "facility_id", "restaurant_name", "address", "zip_code",
    "inspection_date", "score", "process_description",
]


def run():
    with track_run("ingest_inspections", "bronze") as state, engine.begin() as conn:
        conn.execute(text("TRUNCATE bronze.inspections"))
        total = 0
        insert_sql = text(
            f"INSERT INTO bronze.inspections ({', '.join(COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in COLUMNS)}, :raw)"
        )
        for batch in paginate(AUSTIN_INSPECTIONS_URL, order="inspection_date"):
            rows = [
                {**{c: r.get(c) for c in COLUMNS}, "raw": json.dumps(r)}
                for r in batch
            ]
            conn.execute(insert_sql, rows)
            total += len(rows)
            print(f"inspections: {total} rows")
        state["rows"] = total
    print(f"done: {total} rows into bronze.inspections")


if __name__ == "__main__":
    run()
