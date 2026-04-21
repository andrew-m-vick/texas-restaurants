import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/houston_restaurants",
)

TX_MIXED_BEVERAGE_URL = os.getenv(
    "TX_MIXED_BEVERAGE_URL", "https://data.texas.gov/resource/naix-2893.json"
)
HOUSTON_INSPECTIONS_URL = os.getenv("HOUSTON_INSPECTIONS_URL", "")
HOUSTON_VIOLATIONS_URL = os.getenv("HOUSTON_VIOLATIONS_URL", "")

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
HOUSTON_CITY_NAME = os.getenv("HOUSTON_CITY_NAME", "HOUSTON")

PAGE_SIZE = 50_000
