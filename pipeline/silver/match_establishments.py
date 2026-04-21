"""Fuzzy-match mixed beverage locations to inspection facilities, per city.

Strategy (blocked fuzzy join — the interview-worthy bit):
1. Collapse inspections to distinct facilities (one row per facility_id).
2. Block by (city, ZIP). Restaurants in different blocks are never matched.
3. Within each block, score (name_key, address_key) pairs with rapidfuzz
   under a weighted composite (0.6 * name + 0.4 * address).
4. Greedy assignment with removal: for each MB row, pick the best inspection
   candidate above the threshold, then remove that candidate so it can't be
   matched twice.
5. Emit silver.establishments with provenance (match_score, match_method)
   so gold-layer analysis can filter by match confidence.
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


def _match_block(city: str, mb_block: pd.DataFrame, insp_block: pd.DataFrame) -> list[dict]:
    matches = []
    available = insp_block.to_dict("records")
    for _, m in mb_block.iterrows():
        best, best_score, best_idx = None, 0.0, -1
        for i, cand in enumerate(available):
            s = _score(m["name_key"], m["address_key"],
                      cand["name_key"], cand["address_key"])
            if s > best_score:
                best, best_score, best_idx = cand, s, i
        if best and best_score >= THRESHOLD:
            matches.append({
                "city": city,
                "canonical_name": m["location_name"],
                "canonical_address": m["location_address"],
                "zip": m["location_zip"],
                "latitude": best.get("latitude"),
                "longitude": best.get("longitude"),
                "mb_taxpayer_number": m["taxpayer_number"],
                "mb_location_number": m["location_number"],
                "facility_ids": [best["facility_id"]],
                "match_score": round(best_score, 2),
                "match_method": "fuzzy_zip_block",
            })
            available.pop(best_idx)
        else:
            matches.append({
                "city": city,
                "canonical_name": m["location_name"],
                "canonical_address": m["location_address"],
                "zip": m["location_zip"],
                "latitude": None,
                "longitude": None,
                "mb_taxpayer_number": m["taxpayer_number"],
                "mb_location_number": m["location_number"],
                "facility_ids": [],
                "match_score": round(best_score, 2) if best else 0.0,
                "match_method": "mb_only",
            })
    for cand in available:
        matches.append({
            "city": city,
            "canonical_name": cand["restaurant_name"],
            "canonical_address": cand["address"],
            "zip": cand["zip"],
            "latitude": cand.get("latitude"),
            "longitude": cand.get("longitude"),
            "mb_taxpayer_number": None,
            "mb_location_number": None,
            "facility_ids": [cand["facility_id"]],
            "match_score": 0.0,
            "match_method": "inspection_only",
        })
    return matches


def run():
    with track_run("build_silver_layer", "silver.establishments") as state:
        mb = pd.read_sql(
            "SELECT DISTINCT city, taxpayer_number, location_number, location_name, "
            "location_address, location_zip, name_key, address_key "
            "FROM silver.mixed_beverage WHERE location_zip IS NOT NULL",
            engine,
        )
        insp = pd.read_sql(
            """
            SELECT DISTINCT ON (city, facility_id)
                city, facility_id, restaurant_name, address, zip,
                latitude, longitude, name_key, address_key
            FROM silver.inspections
            WHERE zip IS NOT NULL
            ORDER BY city, facility_id, inspection_date DESC
            """,
            engine,
        )

        all_rows: list[dict] = []
        for (city, zip_code), mb_block in mb.groupby(["city", "location_zip"]):
            insp_block = insp[(insp["city"] == city) & (insp["zip"] == zip_code)]
            all_rows.extend(_match_block(city, mb_block, insp_block))

        # Inspection-only blocks (ZIPs with inspections but no MB records)
        mb_blocks = set(zip(mb["city"], mb["location_zip"]))
        for (city, z), grp in insp.groupby(["city", "zip"]):
            if (city, z) in mb_blocks:
                continue
            for cand in grp.to_dict("records"):
                all_rows.append({
                    "city": city,
                    "canonical_name": cand["restaurant_name"],
                    "canonical_address": cand["address"],
                    "zip": cand["zip"],
                    "latitude": cand.get("latitude"),
                    "longitude": cand.get("longitude"),
                    "mb_taxpayer_number": None,
                    "mb_location_number": None,
                    "facility_ids": [cand["facility_id"]],
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
    print(f"silver.establishments: {len(df)} rows "
          f"({dict(df['city'].value_counts()) if len(df) else {}})")


if __name__ == "__main__":
    run()
