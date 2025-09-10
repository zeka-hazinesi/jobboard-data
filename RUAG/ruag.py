#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import json
import re
import sys
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.ruag.ch/en/working-us/job-portal"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

RESULTS_RE = re.compile(r"(\d+)\s+Results\s+found", re.I)
WORKLOAD_RE = re.compile(r"(\d{1,3}(?:[–-]\d{1,3})?%)\s*$")
MFDFLAG_RE = re.compile(r"\bm/f/d\b", re.I)

def parse_total_results(soup: BeautifulSoup) -> Optional[int]:
    # Look for the “### 122 Results found” text block
    text = soup.get_text(" ", strip=True)
    m = RESULTS_RE.search(text)
    return int(m.group(1)) if m else None

def extract_jobs_from_page(soup: BeautifulSoup) -> List[Dict]:
    """
    On each page, job items are linked to jobs.ruag.ch.
    We collect all <a> tags that point to that domain and parse their text.
    Example anchor text:
      "System Manager m/f/d Berufserfahrene Bern Schweiz 80–100%"
      "Senior Network Software Development Engineer Linux / C++ m/f/d Experienced professionals Zürich Seebach Switzerland 100%"

    We’ll parse:
      - title (up to and excluding 'm/f/d')
      - experience (tokens between 'm/f/d' and the first location token)
      - locations (tokens before the country)
      - country (token right before workload; commonly “Schweiz” or “Switzerland”)
      - workload (last token like “80–100%” or “100%”)
    """
    jobs = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "jobs.ruag.ch" not in href:
            continue

        # Clean text spacing
        text = " ".join(a.get_text(" ", strip=True).split())
        if not text:
            continue

        # Identify workload (last token like “80–100%” or “100%”)
        workload = None
        m = WORKLOAD_RE.search(text)
        if m:
            workload = m.group(1)
            text_wo_workload = text[:m.start()].strip()
        else:
            text_wo_workload = text

        # Split by spaces for further parsing
        tokens = text_wo_workload.split()

        # Find 'm/f/d' position to split title vs the rest
        mfds = [i for i, t in enumerate(tokens) if MFDFLAG_RE.fullmatch(t)]
        if mfds:
            mfd_idx = mfds[-1]  # use last occurrence if multiple
            title = " ".join(tokens[:mfd_idx]).strip(" -–—")
            after = tokens[mfd_idx + 1 :]
        else:
            # Fallback: if no 'm/f/d' in text, take a best-effort title
            title = text_wo_workload
            after = []

        # Heuristic: Country is the token right before workload; if we
        # removed workload already, it's now the last token of 'after'.
        country = None
        if after:
            country = after[-1]
            rest = after[:-1]
        else:
            rest = []

        # The remaining 'rest' tokens are typically: experience + locations (maybe multiple words)
        # Experience is one of the categories shown on the page (e.g., “Berufserfahrene”, “Experienced professionals”,
        # “Studierende”, “Berufseinsteigende”, “Young professionals”, “School leavers”).
        # We’ll try to detect a leading experience phrase from a small dictionary of known starts.
        EXPERIENCE_HINTS = [
            "Berufserfahrene", "Berufseinsteigende", "Studierende",
            "Experienced", "professionals", "Young", "School", "leavers",
            "Professionisti", "esperti",  # Italian seen on the page
        ]

        def split_experience_and_locations(words: List[str]):
            if not words:
                return None, []
            # If the first token looks like experience (matches hints), consume tokens
            # until we hit something that looks like a place (capitalized or contains comma).
            # However, places can also be capitalized regularly; this is fuzzy.
            # We’ll consume a leading run of experience-looking tokens.
            exp_tokens = []
            i = 0
            while i < len(words):
                w = words[i]
                if any(w.startswith(h) for h in EXPERIENCE_HINTS):
                    exp_tokens.append(w)
                    i += 1
                else:
                    break
            experience = " ".join(exp_tokens) if exp_tokens else None
            locations_tokens = words[i:]
            return experience, locations_tokens

        experience, loc_tokens = split_experience_and_locations(rest)

        # Sometimes there are multiple locations separated by commas, or joined by commas in the text.
        locations = " ".join(loc_tokens).strip(", ")
        # Normalize country if it fused into locations by mistake
        if country and locations.endswith(" " + country):
            locations = locations[: -(len(country) + 1)].strip(", ")

        jobs.append({
            "title": title,
            "experience": experience,
            "locations": locations if locations else None,
            "country": country,
            "workload": workload,
            "url": href
        })
    return jobs

def fetch_page(page: int) -> BeautifulSoup:
    url = f"{BASE_URL}?page={page}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")

def main():
    out_file = "ruag_jobs.json"
    all_jobs: List[Dict] = []

    # First page: get total results (to compute pages) and scrape
    soup0 = fetch_page(0)
    total = parse_total_results(soup0)  # e.g., 122
    page_jobs = extract_jobs_from_page(soup0)
    all_jobs.extend(page_jobs)

    if total:
        per_page = 20  # observed on the site
        pages = math.ceil(total / per_page)
        print(f"[i] Total results reported: {total}; pages: {pages}")
        start_page = 1
        end_page = pages - 1  # already did page 0
    else:
        # Fallback: unknown total; keep going until a page yields zero jobs
        print("[!] Could not read total results. Falling back to sentinel-based pagination.")
        start_page = 1
        end_page = 999  # upper bound; we will break on empty page

    # Remaining pages
    for p in range(start_page, end_page + 1):
        try:
            soup = fetch_page(p)
        except Exception as e:
            print(f"[!] Error fetching page {p}: {e}", file=sys.stderr)
            break

        jobs = extract_jobs_from_page(soup)
        print(f"[+] Page {p}: {len(jobs)} jobs")
        if not jobs:
            # stop if a page returns no jobs (useful in fallback mode)
            break
        all_jobs.extend(jobs)

    # De-duplicate by URL (in case the portal shows the same item in multiple locations)
    dedup: Dict[str, Dict] = {}
    for j in all_jobs:
        key = j.get("url") or json.dumps(j, sort_keys=True)
        dedup[key] = j
    deduped_jobs = list(dedup.values())

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(deduped_jobs, f, ensure_ascii=False, indent=2)

    print(f"[✓] Collected {len(deduped_jobs)} jobs")
    print(f"[✓] Saved to {out_file}")

if __name__ == "__main__":
    main()
