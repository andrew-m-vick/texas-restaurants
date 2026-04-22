"""Attach TABC licenses to existing silver.establishments.

Runs after match_establishments.py. Strategy:
1. Load silver.establishments (already fuzzy-matched across MB + inspections).
2. Load silver.licenses.
3. Block by (city, zip). Score license.name_key vs establishment.canonical_name
   with the same weighted composite used for MB/inspection matching.
4. For each license, pick the best establishment candidate above threshold.
   Writes to silver.establishment_licenses. Licenses with no match are left
   unattached — they're still queryable from silver.licenses for ZIP-level
   rollups, they just don't surface on any establishment detail page.
"""
import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy import text
from ..db import engine
from ..ops import track_run
from .keys import normalize_name, normalize_address

NAME_WEIGHT = 0.6
ADDR_WEIGHT = 0.4
THRESHOLD = 78.0


def _score(a_name: str, a_addr: str, b_name: str, b_addr: str) -> float:
    n = fuzz.token_set_ratio(a_name, b_name)
    a = fuzz.token_set_ratio(a_addr, b_addr) if a_addr and b_addr else n
    return NAME_WEIGHT * n + ADDR_WEIGHT * a


def run():
    with track_run("build_silver_layer", "silver.establishment_licenses") as state:
        est = pd.read_sql(
            "SELECT id, city, canonical_name, canonical_address, zip "
            "FROM silver.establishments WHERE zip IS NOT NULL",
            engine,
        )
        lic = pd.read_sql(
            "SELECT license_id, city, trade_name, address, zip, name_key, address_key "
            "FROM silver.licenses WHERE zip IS NOT NULL",
            engine,
        )

        if est.empty or lic.empty:
            print("no establishments or licenses to match")
            return

        est["name_key"] = est["canonical_name"].map(normalize_name)
        est["address_key"] = est["canonical_address"].map(normalize_address)

        matches: list[dict] = []
        for (city, zip_code), lic_block in lic.groupby(["city", "zip"]):
            est_block = est[(est["city"] == city) & (est["zip"] == zip_code)]
            if est_block.empty:
                continue
            est_records = est_block.to_dict("records")
            for _, l in lic_block.iterrows():
                best, best_score = None, 0.0
                for e in est_records:
                    s = _score(l["name_key"], l["address_key"],
                               e["name_key"], e["address_key"])
                    if s > best_score:
                        best, best_score = e, s
                if best and best_score >= THRESHOLD:
                    matches.append({
                        "establishment_id": best["id"],
                        "license_id": l["license_id"],
                        "match_score": round(best_score, 2),
                    })

        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.establishment_licenses"))
        if matches:
            pd.DataFrame(matches).to_sql(
                "establishment_licenses", engine, schema="silver",
                if_exists="append", index=False, method="multi", chunksize=2000,
            )
        state["rows"] = len(matches)
    print(f"silver.establishment_licenses: {len(matches)} license↔establishment links")


if __name__ == "__main__":
    run()
