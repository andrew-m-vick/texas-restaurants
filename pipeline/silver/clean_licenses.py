"""bronze.licenses -> silver.licenses."""
from sqlalchemy import text
import pandas as pd
from ..db import engine
from ..ops import track_run
from .keys import normalize_name, normalize_address, zip5


def run():
    with track_run("build_silver_layer", "silver.licenses") as state:
        df = pd.read_sql("SELECT * FROM bronze.licenses", engine)
        if df.empty:
            print("no bronze license rows")
            return

        df["city"] = df["city"].str.upper()
        df["zip"] = df["zip"].map(zip5)
        for c in ["original_issue_date", "current_issued_date",
                  "expiration_date", "status_change_date"]:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

        df["name_key"] = df["trade_name"].map(normalize_name)
        df["address_key"] = df["address"].map(normalize_address)

        df = df.dropna(subset=["license_id", "trade_name"])
        df = df.drop_duplicates(subset=["license_id"])

        keep = [
            "city", "master_file_id", "license_id", "license_type", "tier",
            "primary_status", "trade_name", "owner", "address", "zip",
            "original_issue_date", "current_issued_date",
            "expiration_date", "status_change_date",
            "gun_sign", "name_key", "address_key",
        ]
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.licenses RESTART IDENTITY"))
        df[keep].to_sql(
            "licenses", engine, schema="silver",
            if_exists="append", index=False, method="multi", chunksize=2000,
        )
        state["rows"] = len(df)
    print(f"silver.licenses: {len(df)} rows across {df['city'].nunique()} cities")


if __name__ == "__main__":
    run()
