"""Unit tests for the fuzzy matcher's normalization and scoring.

These cover the interview-worthy bit of the pipeline: the establishment
identity-resolution logic that bridges mixed beverage receipts and
inspection records.
"""
from pipeline.silver.keys import normalize_name, normalize_address, zip5
from pipeline.silver.match_establishments import _score, THRESHOLD


# ---------- normalize_name ----------

class TestNormalizeName:
    def test_strips_llc_inc_corp(self):
        # Apostrophes become spaces during cleanup, so BARNABY'S -> BARNABY S.
        # LLC is stripped as a business suffix.
        assert normalize_name("TORCHYS TACOS LLC") == "TORCHYS TACOS"
        assert normalize_name("Pecan Lodge, Inc.") == "PECAN LODGE"
        assert normalize_name("Big City Corp") == "BIG CITY"

    def test_drops_generic_descriptors(self):
        # "Restaurant" and "Grill" are generic enough to hurt matching
        assert "RESTAURANT" not in normalize_name("Maiko Japanese Restaurant")
        assert "GRILL" not in normalize_name("Pecan Lodge Grill")

    def test_handles_empty_and_none(self):
        assert normalize_name(None) == ""
        assert normalize_name("") == ""
        assert normalize_name("   ") == ""

    def test_idempotent(self):
        once = normalize_name("Maiko L.P. Japanese Restaurant")
        assert normalize_name(once) == once


# ---------- normalize_address ----------

class TestNormalizeAddress:
    def test_abbreviates_street_suffixes(self):
        assert normalize_address("123 Main Street") == "123 MAIN ST"
        assert normalize_address("456 Oak Avenue") == "456 OAK AVE"
        assert normalize_address("789 Congress Boulevard") == "789 CONGRESS BLVD"

    def test_abbreviates_directionals(self):
        assert normalize_address("100 North Main") == "100 N MAIN"
        # "5th" stays as "5TH" (ordinal suffixes are intentionally preserved
        # since they disambiguate street numbers in Austin).
        assert normalize_address("200 West 5th Street") == "200 W 5TH ST"

    def test_strips_punctuation(self):
        assert normalize_address("1234 Main St., Ste. #5") == "1234 MAIN ST STE 5"

    def test_handles_empty(self):
        assert normalize_address(None) == ""
        assert normalize_address("") == ""


# ---------- zip5 ----------

class TestZip5:
    def test_extracts_five_digits(self):
        assert zip5("78701") == "78701"
        assert zip5("78701-1234") == "78701"
        assert zip5("78701 1234") == "78701"

    def test_handles_invalid(self):
        assert zip5("abc") is None
        assert zip5("") is None
        assert zip5(None) is None

    def test_handles_short_inputs(self):
        assert zip5("787") is None


# ---------- scoring ----------

class TestScore:
    def test_identical_strings_score_100(self):
        assert _score("TORCHYS TACOS", "123 MAIN ST",
                     "TORCHYS TACOS", "123 MAIN ST") == 100.0

    def test_weighting_favors_name(self):
        # Same name, different address → should still score high
        high_name_only = _score("MAIKO SUSHI", "", "MAIKO SUSHI", "")
        low_name_different_addr = _score(
            "MAIKO SUSHI", "311 W 6 ST", "MAIKO SUSHI", "900 E 41 ST"
        )
        # When addresses are present but different, score drops from 100
        assert low_name_different_addr < high_name_only
        # But name similarity still carries 60% weight, so score stays decent
        assert low_name_different_addr > 50

    def test_real_world_fuzzy_match(self):
        # The canonical case: one source has the legal entity, the other has the DBA
        score = _score(
            normalize_name("MAIKO LP"),
            normalize_address("311 W 6TH ST"),
            normalize_name("MAIKO SUSHI LOUNGE"),
            normalize_address("311 W 6TH ST"),
        )
        assert score >= THRESHOLD, f"expected above-threshold match, got {score}"

    def test_different_establishments_score_low(self):
        # Two completely unrelated restaurants in the same ZIP
        score = _score(
            normalize_name("PECAN LODGE BARBECUE"),
            normalize_address("2702 MAIN ST"),
            normalize_name("CHILIS GRILL AND BAR"),
            normalize_address("789 OTHER ST"),
        )
        assert score < THRESHOLD, f"expected sub-threshold non-match, got {score}"

    def test_empty_addresses_fall_back_to_name_score(self):
        # If either side lacks address, scorer treats address score as name score
        score = _score("TORCHYS TACOS", "", "TORCHYS TACOS", "")
        assert score == 100.0
