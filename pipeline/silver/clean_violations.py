"""bronze.dallas_violations -> silver.violations.

Austin doesn't publish violation detail; this silver table only has Dallas rows.
"""
import pandas as pd
from sqlalchemy import text
from ..db import engine
from ..ops import track_run


def run():
    with track_run("build_silver_layer", "silver.violations") as state:
        df = pd.read_sql(
            """
            SELECT facility_id, inspection_date, description, points, memo
            FROM bronze.dallas_violations
            """,
            engine,
        )
        if df.empty:
            print("no dallas violations in bronze")
            state["rows"] = 0
            return
        df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce").dt.date
        df["points"] = pd.to_numeric(df["points"], errors="coerce")
        df["city"] = "DALLAS"
        df = df.dropna(subset=["description", "facility_id"])

        keep = ["city", "facility_id", "inspection_date", "description", "points", "memo"]
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.violations RESTART IDENTITY"))
        df[keep].to_sql(
            "violations", engine, schema="silver",
            if_exists="append", index=False, method="multi", chunksize=5000,
        )
        state["rows"] = len(df)
    print(f"silver.violations: {len(df)} rows")


if __name__ == "__main__":
    run()
