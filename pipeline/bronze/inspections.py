"""Ingest City of Houston food service inspections + violations.

Dataset IDs in data.houstontx.gov vary — configure the URLs in .env.
We map flexibly: the exact field names differ by portal version, so we pull
everything into `raw` JSONB and best-effort into typed columns.
"""
import json
from sqlalchemy import text
from ..config import HOUSTON_INSPECTIONS_URL, HOUSTON_VIOLATIONS_URL
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

# Houston portal column aliases — adjust in .env-driven config if needed.
INSP_MAP = {
    "source_id": ["inspection_id", "id", ":id"],
    "establishment_name": ["establishment_name", "business_name", "name", "dba"],
    "address": ["address", "site_address", "street_address"],
    "city": ["city"],
    "zip": ["zip", "zipcode", "postal_code"],
    "inspection_date": ["inspection_date", "insp_date", "date"],
    "inspection_type": ["inspection_type", "type"],
    "score": ["score", "inspection_score"],
    "grade": ["grade"],
    "latitude": ["latitude", "lat"],
    "longitude": ["longitude", "lon", "lng"],
}

VIOL_MAP = {
    "source_id": ["violation_id", "id", ":id"],
    "inspection_source_id": ["inspection_id", "insp_id"],
    "establishment_name": ["establishment_name", "business_name", "name"],
    "address": ["address", "site_address"],
    "violation_code": ["violation_code", "code"],
    "violation_description": ["violation_description", "description", "violation"],
    "violation_date": ["violation_date", "date"],
    "severity": ["severity", "priority"],
}


def _pick(row: dict, keys: list[str]):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def _load(url: str, table: str, field_map: dict[str, list[str]]):
    if not url:
        print(f"skip {table}: URL not configured")
        return 0
    cols = list(field_map.keys())
    insert_sql = text(
        f"INSERT INTO bronze.{table} ({', '.join(cols)}, raw) "
        f"VALUES ({', '.join(':' + c for c in cols)}, :raw)"
    )
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE bronze.{table}"))
        total = 0
        for batch in paginate(url):
            rows = []
            for r in batch:
                row = {c: _pick(r, field_map[c]) for c in cols}
                row["raw"] = json.dumps(r)
                rows.append(row)
            conn.execute(insert_sql, rows)
            total += len(rows)
            print(f"{table}: {total} rows")
    return total


def run():
    with track_run("ingest_inspections", "bronze") as state:
        n1 = _load(HOUSTON_INSPECTIONS_URL, "inspections", INSP_MAP)
        n2 = _load(HOUSTON_VIOLATIONS_URL, "violations", VIOL_MAP)
        state["rows"] = n1 + n2
    print(f"done: {n1} inspections, {n2} violations")


if __name__ == "__main__":
    run()
