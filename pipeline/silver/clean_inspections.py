"""bronze.inspections -> silver.inspections (unified across cities)."""
import pandas as pd
from sqlalchemy import text
from ..db import engine
from ..ops import track_run
from .keys import normalize_name, normalize_address, zip5


def run():
    with track_run("build_silver_layer", "silver.inspections") as state:
        df = pd.read_sql("SELECT * FROM bronze.inspections", engine)
        if df.empty:
            print("no bronze inspections rows")
            state["rows"] = 0
            return

        df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce").dt.date
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["zip"] = df["zip_code"].map(zip5)
        df["name_key"] = df["restaurant_name"].map(normalize_name)
        df["address_key"] = df["address"].map(normalize_address)

        df = df.dropna(subset=["restaurant_name", "facility_id"])
        df["inspection_type"] = df["inspection_type"].fillna("Routine")
        df = df.drop_duplicates(
            subset=["city", "facility_id", "inspection_date", "inspection_type"]
        )

        keep = [
            "city", "facility_id", "restaurant_name", "address", "zip",
            "inspection_date", "score", "inspection_type", "latitude", "longitude",
            "name_key", "address_key",
        ]
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.inspections RESTART IDENTITY"))
        df[keep].to_sql(
            "inspections", engine, schema="silver",
            if_exists="append", index=False, method="multi", chunksize=5000,
        )
        state["rows"] = len(df)
    print(f"silver.inspections: {len(df)} rows "
          f"({dict(df['city'].value_counts())})")


if __name__ == "__main__":
    run()
