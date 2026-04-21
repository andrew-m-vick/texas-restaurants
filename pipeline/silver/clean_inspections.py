"""bronze.inspections + violations -> silver."""
import pandas as pd
from sqlalchemy import text
from ..db import engine
from ..ops import track_run
from .keys import normalize_name, normalize_address, zip5


def _clean_inspections():
    df = pd.read_sql("SELECT * FROM bronze.inspections", engine)
    if df.empty:
        return 0
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce").dt.date
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["zip"] = df["zip"].map(zip5)
    df["name_key"] = df["establishment_name"].map(normalize_name)
    df["address_key"] = df["address"].map(normalize_address)
    df = df.dropna(subset=["establishment_name"])
    df = df.drop_duplicates(subset=["source_id"])

    keep = [
        "source_id", "establishment_name", "address", "zip",
        "inspection_date", "inspection_type", "score", "grade",
        "latitude", "longitude", "name_key", "address_key",
    ]
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.inspections RESTART IDENTITY"))
    df[keep].to_sql(
        "inspections", engine, schema="silver",
        if_exists="append", index=False, method="multi", chunksize=5000,
    )
    return len(df)


def _clean_violations():
    df = pd.read_sql("SELECT * FROM bronze.violations", engine)
    if df.empty:
        return 0
    df["violation_date"] = pd.to_datetime(df["violation_date"], errors="coerce").dt.date
    df = df.drop_duplicates(subset=["source_id"])
    keep = [
        "source_id", "inspection_source_id", "violation_code",
        "violation_description", "violation_date", "severity",
    ]
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE silver.violations RESTART IDENTITY"))
    df[keep].to_sql(
        "violations", engine, schema="silver",
        if_exists="append", index=False, method="multi", chunksize=5000,
    )
    return len(df)


def run():
    with track_run("build_silver_layer", "silver.inspections") as state:
        n1 = _clean_inspections()
        n2 = _clean_violations()
        state["rows"] = n1 + n2
    print(f"silver.inspections={n1} silver.violations={n2}")


if __name__ == "__main__":
    run()
