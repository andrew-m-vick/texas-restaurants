import os
from dotenv import load_dotenv

load_dotenv()

_raw_db_url = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/austin_restaurants",
)
if _raw_db_url.startswith("postgresql://"):
    _raw_db_url = "postgresql+psycopg2://" + _raw_db_url[len("postgresql://"):]
elif _raw_db_url.startswith("postgres://"):
    _raw_db_url = "postgresql+psycopg2://" + _raw_db_url[len("postgres://"):]
DATABASE_URL = _raw_db_url

TX_MIXED_BEVERAGE_URL = os.getenv(
    "TX_MIXED_BEVERAGE_URL", "https://data.texas.gov/resource/naix-2893.json"
)
AUSTIN_INSPECTIONS_URL = os.getenv(
    "AUSTIN_INSPECTIONS_URL", "https://data.austintexas.gov/resource/ecmv-9xxi.json"
)

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
TARGET_CITIES = [
    c.strip().upper()
    for c in os.getenv("TARGET_CITIES", "AUSTIN").split(",")
    if c.strip()
]

PAGE_SIZE = 50_000
