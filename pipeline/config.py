import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/austin_restaurants",
)

TX_MIXED_BEVERAGE_URL = os.getenv(
    "TX_MIXED_BEVERAGE_URL", "https://data.texas.gov/resource/naix-2893.json"
)
AUSTIN_INSPECTIONS_URL = os.getenv(
    "AUSTIN_INSPECTIONS_URL", "https://data.austintexas.gov/resource/ecmv-9xxi.json"
)
DALLAS_INSPECTIONS_URL = os.getenv(
    "DALLAS_INSPECTIONS_URL", "https://www.dallasopendata.com/resource/dri5-wcct.json"
)

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
TARGET_CITIES = [
    c.strip().upper()
    for c in os.getenv("TARGET_CITIES", "AUSTIN,DALLAS").split(",")
    if c.strip()
]

PAGE_SIZE = 50_000
