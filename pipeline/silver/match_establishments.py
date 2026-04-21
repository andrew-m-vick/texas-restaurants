"""Fuzzy-match mixed beverage locations to inspection establishments.

Strategy (blocked fuzzy join — the interview-worthy bit):
1. Block by ZIP. Restaurants in different ZIPs are not the same establishment.
2. Within a ZIP block, use rapidfuzz.process.cdist on (name_key, address_key)
   to compute a composite similarity score.
3. Use Hungarian-style greedy assignment: for each MB row, pick the best
   inspection candidate above the threshold, then remove that candidate so
   it can't be matched twice.
4. Emit a unified `silver.establishments` table with provenance (match_score,
   match_method) so gold-layer analysis can filter by match confidence.

Records that can't be confidently matched still get written — as one-sided
rows — so downstream reports don't silently drop data.
"""
import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy import text
from ..db import engine
from ..ops import track_run

NAME_WEIGHT = 0.6
ADDR_WEIGHT = 0.4
THRESHOLD = 78.0


def _score(mb_name: str, mb_addr: str, in_name: str, in_addr: str) -> float:
    n = fuzz.token_set_ratio(mb_name, in_name)
    a = fuzz.token_set_ratio(mb_addr, in_addr) if mb_addr and in_addr else n
    return NAME_WEIGHT * n + ADDR_WEIGHT * a


def _match_block(mb_block: pd.DataFrame, insp_block: pd.DataFrame) -> list[dict]:
    matches = []
    available = insp_block.to_dict("records")
    for _, m in mb_block.iterrows():
        best = None
        best_score = 0.0
        best_idx = -1
        for i, cand in enumerate(available):
            s = _score(m["name_key"], m["address_key"],
                      cand["name_key"], cand["address_key"])
            if s > best_score:
                best_score = s
                best = cand
                best_idx = i
        if best and best_score >= THRESHOLD:
            matches.append({
                "canonical_name": m["location_name"],
                "canonical_address": m["location_address"],
                "zip": m["location_zip"],
                "latitude": best.get("latitude"),
                "longitude": best.get("longitude"),
                "mb_taxpayer_number": m["taxpayer_number"],
                "mb_location_number": m["location_number"],
                "inspection_source_ids": [best["source_id"]],
                "match_score": round(best_score, 2),
                "match_method": "fuzzy_zip_block",
            })
            available.pop(best_idx)
        else:
            matches.append({
                "canonical_name": m["location_name"],
                "canonical_address": m["location_address"],
                "zip": m["location_zip"],
                "latitude": None,
                "longitude": None,
                "mb_taxpayer_number": m["taxpayer_number"],
                "mb_location_number": m["location_number"],
                "inspection_source_ids": [],
                "match_score": round(best_score, 2) if best else 0.0,
                "match_method": "mb_only",
            })
    # Inspection-only rows (no MB match)
    for cand in available:
        matches.append({
            "canonical_name": cand["establishment_name"],
            "canonical_address": cand["address"],
            "zip": cand["zip"],
            "latitude": cand.get("latitude"),
            "longitude": cand.get("longitude"),
            "mb_taxpayer_number": None,
            "mb_location_number": None,
            "inspection_source_ids": [cand["source_id"]],
            "match_score": 0.0,
            "match_method": "inspection_only",
        })
    return matches


def run():
    with track_run("build_silver_layer", "silver.establishments") as state:
        mb = pd.read_sql(
            "SELECT DISTINCT taxpayer_number, location_number, location_name, "
            "location_address, location_zip, name_key, address_key "
            "FROM silver.mixed_beverage WHERE location_zip IS NOT NULL",
            engine,
        )
        insp = pd.read_sql(
            "SELECT DISTINCT ON (name_key, address_key, zip) "
            "source_id, establishment_name, address, zip, "
            "latitude, longitude, name_key, address_key "
            "FROM silver.inspections WHERE zip IS NOT NULL",
            engine,
        )
        all_rows: list[dict] = []
        for zip_code, mb_block in mb.groupby("location_zip"):
            insp_block = insp[insp["zip"] == zip_code]
            all_rows.extend(_match_block(mb_block, insp_block))
        # Inspection-only ZIPs (no MB records in that ZIP at all)
        missing_zips = set(insp["zip"].unique()) - set(mb["location_zip"].unique())
        for z in missing_zips:
            insp_block = insp[insp["zip"] == z]
            for cand in insp_block.to_dict("records"):
                all_rows.append({
                    "canonical_name": cand["establishment_name"],
                    "canonical_address": cand["address"],
                    "zip": cand["zip"],
                    "latitude": cand.get("latitude"),
                    "longitude": cand.get("longitude"),
                    "mb_taxpayer_number": None,
                    "mb_location_number": None,
                    "inspection_source_ids": [cand["source_id"]],
                    "match_score": 0.0,
                    "match_method": "inspection_only",
                })

        df = pd.DataFrame(all_rows)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE silver.establishments RESTART IDENTITY"))
        if not df.empty:
            df.to_sql(
                "establishments", engine, schema="silver",
                if_exists="append", index=False, method="multi", chunksize=2000,
            )
        state["rows"] = len(df)
    print(f"silver.establishments: {len(df)} rows")


if __name__ == "__main__":
    run()
