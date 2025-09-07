#!/usr/bin/env python3
# fetch_jobs_paginated.py
import json
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://ohws.prospective.ch/public/v1/medium/1000666/jobs"
LANG = "de"
LIMIT = 200  # API only allows up to 200
OUT_FILE = Path("jobs.json")
MAX_PAGES = 50  # safety guard

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobFetcher/1.0)",
    "Accept": "application/json",
}

RETRY_STATUSES = {429, 500, 502, 503, 504}


def extract_items(payload):
    """Return a list of job items from various possible response shapes."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("jobs", "items", "results", "data"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]
    return []


def fetch_page(offset):
    """Fetch a single page with retries."""
    params = {"lang": LANG, "offset": offset, "limit": LIMIT}
    backoff = 1.0
    for attempt in range(5):
        try:
            r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
            if r.status_code in RETRY_STATUSES:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == 4:
                raise
            time.sleep(backoff)
            backoff *= 2
    # Unreachable
    return None


def main():
    all_jobs = []
    seen_ids = set()

    total_fetched = 0
    for page in range(MAX_PAGES):
        offset = page * LIMIT
        payload = fetch_page(offset)
        items = extract_items(payload)

        # De-duplicate by "id" if present
        new_items = []
        for it in items:
            jid = it.get("id") if isinstance(it, dict) else None
            if jid is None or jid not in seen_ids:
                new_items.append(it)
                if jid is not None:
                    seen_ids.add(jid)

        all_jobs.extend(new_items)
        total_fetched += len(items)
        print(f"[page {page+1}] offset={offset} got={len(items)} unique_added={len(new_items)}")

        # Stop when the current page returns fewer than LIMIT items
        if len(items) < LIMIT:
            break

    # Write combined JSON
    with OUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(all_jobs, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(all_jobs)} jobs to {OUT_FILE} (fetched {total_fetched} raw items across {page+1} page(s)).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Failed: {e}", file=sys.stderr)
        sys.exit(1)
