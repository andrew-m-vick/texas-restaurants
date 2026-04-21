"""Thin Socrata-style paginator (works for data.texas.gov and most Houston portals)."""
import time
from typing import Iterator
import requests
from .config import SOCRATA_APP_TOKEN, PAGE_SIZE


def paginate(url: str, where: str | None = None, order: str = ":id") -> Iterator[list[dict]]:
    headers = {"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}
    offset = 0
    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": order}
        if where:
            params["$where"] = where
        resp = requests.get(url, params=params, headers=headers, timeout=120)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            return
        yield batch
        if len(batch) < PAGE_SIZE:
            return
        offset += PAGE_SIZE
        time.sleep(0.1)
