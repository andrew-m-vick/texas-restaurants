"""Shared normalization for fuzzy matching keys."""
import re

_BUSINESS_STOPWORDS = {
    "LLC", "INC", "CORP", "CORPORATION", "CO", "COMPANY", "LP", "LLP",
    "LTD", "THE", "RESTAURANT", "GRILL", "BAR", "CAFE", "KITCHEN",
}
_STREET_ABBR = {
    "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "ROAD": "RD",
    "DRIVE": "DR", "LANE": "LN", "PARKWAY": "PKWY", "HIGHWAY": "HWY",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
}


def normalize_name(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9 ]", " ", s.upper())
    tokens = [t for t in s.split() if t and t not in _BUSINESS_STOPWORDS]
    return " ".join(tokens)


def normalize_address(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9 ]", " ", s.upper())
    tokens = [_STREET_ABBR.get(t, t) for t in s.split() if t]
    return " ".join(tokens)


def zip5(z: str | None) -> str | None:
    if not z:
        return None
    m = re.match(r"(\d{5})", str(z))
    return m.group(1) if m else None
