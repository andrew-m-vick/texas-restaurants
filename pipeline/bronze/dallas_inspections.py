"""Ingest Dallas food establishment inspections (dataset dri5-wcct, 2016-2024).

Dallas denormalizes up to 15 violations per inspection row. We:
  1) Write one clean inspection row into bronze.inspections
  2) Explode violation1..violation15 columns into bronze.dallas_violations

The dataset is sunset as of Jan 2024; this is effectively historical data.
"""
import json
from sqlalchemy import text
from ..config import DALLAS_INSPECTIONS_URL
from ..db import engine
from ..ops import track_run
from ..socrata import paginate

INSP_COLUMNS = [
    "city", "facility_id", "restaurant_name", "address", "zip_code",
    "inspection_date", "score", "inspection_type", "latitude", "longitude",
]
VIOL_COLUMNS = [
    "facility_id", "inspection_date", "violation_number", "description",
    "points", "tfer_text", "memo",
]


def _lat_lon(r: dict) -> tuple[str | None, str | None]:
    ll = r.get("lat_long") or {}
    return ll.get("latitude"), ll.get("longitude")


def _explode_violations(r: dict, facility_id: str, inspection_date: str) -> list[dict]:
    out = []
    for i in range(1, 16):
        desc = r.get(f"violation{i}_description")
        if not desc:
            continue
        out.append({
            "facility_id": facility_id,
            "inspection_date": inspection_date,
            "violation_number": i,
            "description": desc,
            "points": r.get(f"violation{i}_points"),
            "tfer_text": r.get(f"violation{i}_text"),
            "memo": r.get(f"violation{i}_memo"),
        })
    return out


def run():
    with track_run("ingest_dallas_inspections", "bronze") as state, engine.begin() as conn:
        conn.execute(text("DELETE FROM bronze.inspections WHERE city = 'DALLAS'"))
        conn.execute(text("TRUNCATE bronze.dallas_violations"))

        insp_sql = text(
            f"INSERT INTO bronze.inspections ({', '.join(INSP_COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in INSP_COLUMNS)}, :raw)"
        )
        viol_sql = text(
            f"INSERT INTO bronze.dallas_violations ({', '.join(VIOL_COLUMNS)}, raw) "
            f"VALUES ({', '.join(':' + c for c in VIOL_COLUMNS)}, :raw)"
        )

        total_insp, total_viol = 0, 0
        for batch in paginate(DALLAS_INSPECTIONS_URL, order="insp_date"):
            insp_rows, viol_rows = [], []
            for r in batch:
                fid = r.get("program_identifier")
                dt = r.get("insp_date")
                lat, lon = _lat_lon(r)
                insp_rows.append({
                    "city": "DALLAS",
                    "facility_id": fid,
                    "restaurant_name": r.get("program_identifier"),
                    "address": r.get("site_address"),
                    "zip_code": r.get("zip"),
                    "inspection_date": dt,
                    "score": r.get("score"),
                    "inspection_type": r.get("type"),
                    "latitude": lat,
                    "longitude": lon,
                    "raw": json.dumps(r),
                })
                for v in _explode_violations(r, fid, dt):
                    viol_rows.append({**v, "raw": json.dumps({"src": r.get(":id")})})

            conn.execute(insp_sql, insp_rows)
            if viol_rows:
                conn.execute(viol_sql, viol_rows)
            total_insp += len(insp_rows)
            total_viol += len(viol_rows)
            print(f"dallas_inspections: {total_insp} insp / {total_viol} violations")
        state["rows"] = total_insp + total_viol
    print(f"done: {total_insp} inspections + {total_viol} violations (DALLAS)")


if __name__ == "__main__":
    run()
