#!/usr/bin/env python3
"""
Scrape all Lidl CH job offers into lidl_jobs.json.

- Iterates pages until "result.hits" is empty.
- Dedupes by jobId (falls back to reference if missing).
- Saves all hits (raw) in a single JSON file with some metadata.

Usage:
  python lidl_jobs.py
"""

import json
import time
import sys
from typing import Any, Dict, List, Optional
import requests

BASE_URL = "https://team.lidl.ch/de/search_api/jobsearch"
OUTPUT_FILE = "lidl_jobs.json"

# Empty filters as requested
FILTER = {
    "contract_type": [],
    "employment_area": [],
    "entry_level": [],
    "language": [],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LidlJobsScraper/1.0; +https://example.com)",
    "Accept": "application/json",
}

# Backoff for transient errors / 429s
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds
PAGE_DELAY = 0.2       # politeness delay between pages


def fetch_page(session: requests.Session, page: int) -> Optional[Dict[str, Any]]:
    """Fetch a single page with basic retry/backoff."""
    params = {
        "page": page,
        "filter": json.dumps(FILTER, separators=(",", ":")),
        "with_event": "true",
        # You *might* try a larger page size if the API supports it:
        # "resultsPerPage": 100,
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            if resp.status_code in RETRY_STATUS:
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} on page {page}", response=resp
                )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            if attempt >= MAX_RETRIES:
                print(f"[!] Failed to fetch page {page} after {attempt} attempts: {e}", file=sys.stderr)
                return None
            print(f"[i] Transient error on page {page} (attempt {attempt}/{MAX_RETRIES}): {e}. "
                  f"Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
            backoff *= 2
    return None


def main() -> None:
    session = requests.Session()
    session.headers.update(HEADERS)

    all_hits: List[Dict[str, Any]] = []
    seen_ids = set()

    page = 1
    total_reported = None  # from payload "result.count", optional

    while True:
        data = fetch_page(session, page)
        if data is None:
            # Hard stop on repeated failures
            break

        result = data.get("result", {})
        hits = result.get("hits", []) or []

        if total_reported is None and "count" in result:
            total_reported = result.get("count")

        if not hits:
            print(f"[✓] Page {page}: empty hits -> done.")
            break

        # Deduplicate & collect
        new_count = 0
        for h in hits:
            jid = h.get("jobId") or h.get("reference")
            if jid is None:
                # Keep even if no id — rare — but avoid dup risk
                all_hits.append(h)
                new_count += 1
                continue
            if jid in seen_ids:
                continue
            seen_ids.add(jid)
            all_hits.append(h)
            new_count += 1

        print(f"[+] Page {page}: {len(hits)} hits ({new_count} new). Total collected: {len(all_hits)}")
        page += 1
        time.sleep(PAGE_DELAY)

    output = {
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": BASE_URL,
        "params": {"filter": FILTER, "with_event": True},
        "reported_total": total_reported,
        "collected_count": len(all_hits),
        "hits": all_hits,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Saved {len(all_hits)} job offers to {OUTPUT_FILE}")
    if total_reported is not None:
        print(f"ℹ Reported total in API: {total_reported}")


if __name__ == "__main__":
    main()
