#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Schindler jobs scraper (Switzerland only)
- Paginates with startrow=0,25,50,... based on "Results X-Y of TOTAL" on first page
- Uses requests + BeautifulSoup (bs4)
- Saves JSON to schindler_jobs_ch.json

Install deps:
  pip install requests beautifulsoup4

Run:
  python schindler_scrape.py
"""

import json
import re
import time
from dataclasses import asdict, dataclass
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://job.schindler.com"
SEARCH_PATH = "/search/"
# We keep your requested filters here (CH + sort by date desc)
BASE_QUERY = {
    "q": "",
    "sortColumn": "referencedate",
    "sortDirection": "desc",
    "optionsFacetsDD_country": "CH",
    # startrow will be set dynamically
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SchindlerJobScraper/1.0; +https://example.org/bot)"
}


@dataclass
class Job:
    title: str
    url: str
    location: Optional[str]
    posted: Optional[str]
    job_id: Optional[str]
    source_row: int  # which startrow page it came from


def fetch_html(session: requests.Session, startrow: int) -> str:
    params = dict(BASE_QUERY)
    params["startrow"] = str(startrow)
    resp = session.get(urljoin(BASE, SEARCH_PATH), params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_total_results(html: str) -> Optional[int]:
    """
    Look for phrases like: "Results 1 – 25 of 123" or "Results 1 - 25 of 123"
    We search in the whole text to be resilient against HTML structure changes.
    """
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    m = re.search(r"Results?\s+\d+\s*[-–]\s*\d+\s*of\s*(\d+)", text, flags=re.I)
    if m:
        return int(m.group(1))
    return None


def best_effort_text(elem: Optional[BeautifulSoup]) -> Optional[str]:
    if not elem:
        return None
    t = elem.get_text(" ", strip=True)
    return t or None


def parse_job_id_from_url(url: str) -> Optional[str]:
    """
    Try to pull a numeric ID or last path segment as job_id.
    Common patterns on many career sites include .../job/<slug>/<ID>/ or .../<ID>
    """
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        # heuristics: prefer last numeric token
        nums = [p for p in parts if p.isdigit()]
        if nums:
            return nums[-1]
        # otherwise last segment
        return parts[-1] if parts else None
    except Exception:
        return None


def parse_jobs_from_page(html: str, source_row: int) -> List[Job]:
    """
    We’ll be generous in selectors:
    - Collect <a> elements whose href looks like a job detail (contains '/job/')
    - Derive title from the anchor text
    - Attempt to locate location/posted near the anchor (siblings/parents)
    """
    soup = BeautifulSoup(html, "lxml")

    # First try a more specific container if present
    container = soup.select_one("#search-results-list, .search-results, .jobs-list, .jobs")
    search_scope = container if container else soup

    jobs: List[Job] = []
    seen_links = set()

    # Heuristic: any anchor with href containing '/job/' on this domain
    for a in search_scope.select("a[href]"):
        href = a.get("href", "")
        if "/job/" not in href:
            continue

        # Build absolute URL and de-dup
        url = urljoin(BASE, href)
        if url in seen_links:
            continue
        seen_links.add(url)

        title = a.get_text(" ", strip=True) or None
        if not title:
            # skip anchors with no visible title
            continue

        # Try to find a card/list item wrapper to extract nearby fields
        card = a
        for _ in range(3):
            if card.parent:
                card = card.parent
            else:
                break

        # Location: common patterns
        loc = None
        # look for elements likely to contain location keywords/classes
        loc_candidates = []
        loc_candidates += card.select(".job-location, .location, [data-careersite-propertyid='location']")
        loc_candidates += card.find_all(lambda t: t.name in ("span", "div", "p") and "location" in " ".join(t.get("class", [])).lower() if t.get("class") else False)

        if loc_candidates:
            loc = best_effort_text(loc_candidates[0])
        else:
            # keyword scan in nearby small/spans
            near_texts = [best_effort_text(s) for s in card.select("small, span, div, time")][:6]
            for t in near_texts:
                if t and any(k in t.lower() for k in ["switzerland", "schweiz", "suisse", "svizzera"]) or (t and "," in t and len(t) < 80):
                    loc = t
                    break

        # Posted date
        posted = None
        # time tag is common
        time_el = card.find("time")
        if time_el and (time_el.get("datetime") or time_el.get_text(strip=True)):
            posted = time_el.get("datetime") or time_el.get_text(strip=True)

        # Sometimes date shown as "Posted X" in small text
        if not posted:
            smalls = card.select("small, .job-date, .posting-date, .date")
            for s in smalls:
                txt = best_effort_text(s)
                if txt and re.search(r"\b(Posted|Veröffentlicht|Publiée|Data|Date)\b", txt, flags=re.I):
                    posted = txt
                    break

        jobs.append(
            Job(
                title=title or "",
                url=url,
                location=loc,
                posted=posted,
                job_id=parse_job_id_from_url(url),
                source_row=source_row,
            )
        )

    return jobs


def scrape_all() -> List[Job]:
    session = requests.Session()
    session.headers.update(HEADERS)

    print("[*] Fetching first page to determine total ...")
    first_html = fetch_html(session, startrow=0)

    total = extract_total_results(first_html)
    if total is None:
        print("⚠️  Could not detect total from the first page. Will paginate until a blank page is returned.")
        total = 10**9  # big sentinel, we’ll stop on empty page anyway
    else:
        print(f"[+] Total jobs reported: {total}")

    all_jobs: List[Job] = []
    startrow = 0
    step = 25
    seen_urls = set()

    while startrow < total:
        if startrow == 0:
            html = first_html
        else:
            # polite crawl; adjust if you get rate-limited
            time.sleep(0.6)
            html = fetch_html(session, startrow=startrow)

        page_jobs = parse_jobs_from_page(html, source_row=startrow)
        # Deduplicate by URL across pages
        new_jobs = [j for j in page_jobs if j.url not in seen_urls]
        for j in new_jobs:
            seen_urls.add(j.url)

        print(f"[+] startrow={startrow:>5} → found {len(page_jobs)} (new: {len(new_jobs)}) | total so far: {len(all_jobs) + len(new_jobs)}")
        all_jobs.extend(new_jobs)

        # Stop if the page returned nothing
        if not page_jobs:
            print("[!] Page returned no jobs; stopping early.")
            break

        startrow += step

    return all_jobs


def main():
    jobs = scrape_all()
    out_path = "schindler_jobs_ch.json"
    data = [asdict(j) for j in jobs]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Saved {len(jobs)} jobs to {out_path}")


if __name__ == "__main__":
    main()
