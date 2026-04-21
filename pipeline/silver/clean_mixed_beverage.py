"""bronze.mixed_beverage -> silver.mixed_beverage."""
from sqlalchemy import text
import pandas as pd
from ..db import engine
from ..ops import track_run
from .keys import normalize_name, normalize_address, zip5


def run():
    with track_run("build_silver_layer", "silver.mixed_beverage") as state:
        df = pd.read_sql("SELECT * FROM bronze.mixed_beverage", engine)
        if df.empty:
            print("no bronze mixed_beverage rows")
            return

        df["obligation_end_date"] = pd.to_datetime(
            df["obligation_end_date_yyyymmdd"], errors="coerce"
        ).dt.date
        for c in ["liquor_receipts", "wine_receipts", "beer_receipts",
                  "cover_charge_receipts", "total_receipts"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        df["city"] = df["location_city"].str.upper()
        df["location_zip"] = df["location_zip"].map(zip5)
        df["name_key"] = df["location_name"].map(normalize_name)
        df["address_key"] = df["location_address"].map(normalize_address)

        df = df.dropna(subset=["obligation_end_date", "location_name"])
        df = df.drop_duplicates(
            subset=["taxpayer_number", "location_number", "obligation_end_date"]
        )

        keep = [
            "city", "taxpayer_number", "location_number", "location_name",
            "location_address", "location_zip", "obligation_end_date",
            "liquor_receipts", "wine_receipts", "beer_receipts",
            "cover_charge_receipts", "total_receipts", "name_key", "address_key",
        ]
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.mixed_beverage RESTART IDENTITY"))
        df[keep].to_sql(
            "mixed_beverage", engine, schema="silver",
            if_exists="append", index=False, method="multi", chunksize=5000,
        )
        state["rows"] = len(df)
    print(f"silver.mixed_beverage: {len(df)} rows across {df['city'].nunique()} cities")


if __name__ == "__main__":
    run()
