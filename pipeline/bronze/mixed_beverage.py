"""Ingest Texas Mixed Beverage Gross Receipts into bronze.mixed_beverage.

Filters to Houston at the API level to keep payloads small. Re-runnable:
truncates bronze table and reloads (bronze is disposable by design).
"""
import json
from sqlalchemy import text
from ..config import TX_MIXED_BEVERAGE_URL, TARGET_CITY_NAME
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

COLUMNS = [
    "taxpayer_number", "taxpayer_name", "taxpayer_address", "taxpayer_city",
    "taxpayer_state", "taxpayer_zip", "location_number", "location_name",
    "location_address", "location_city", "location_state", "location_zip",
    "location_county", "obligation_end_date_yyyymmdd", "liquor_receipts",
    "wine_receipts", "beer_receipts", "cover_charge_receipts", "total_receipts",
]


def run():
    where = f"upper(location_city) = '{TARGET_CITY_NAME.upper()}'"
    with track_run("ingest_mixed_beverage", "bronze") as state, engine.begin() as conn:
        conn.execute(text("TRUNCATE bronze.mixed_beverage"))
        total = 0
        insert_sql = text(
            f"INSERT INTO bronze.mixed_beverage ({', '.join(COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in COLUMNS)}, :raw)"
        )
        for batch in paginate(TX_MIXED_BEVERAGE_URL, where=where):
            rows = [
                {**{c: r.get(c) for c in COLUMNS}, "raw": json.dumps(r)}
                for r in batch
            ]
            conn.execute(insert_sql, rows)
            total += len(rows)
            print(f"mixed_beverage: {total} rows")
        state["rows"] = total
    print(f"done: {total} rows into bronze.mixed_beverage")


if __name__ == "__main__":
    run()
