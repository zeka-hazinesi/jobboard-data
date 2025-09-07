"""
Hirslanden (SuccessFactors) job overview scraper — requests + BeautifulSoup only.

- Follows the site's real "More Search Results" link when present.
- Falls back to startrow pagination, auto-detecting per-page from the banner.
- Saves overview only (title, url, facility, city, job_id) to hirslanden_jobs.json.

Usage:
  pip install requests beautifulsoup4 lxml
  python hirslanden.py
"""

from __future__ import annotations
import json
import re
import time
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.parse import urlparse, urlunparse, urlencode, urljoin, parse_qs

import requests
from bs4 import BeautifulSoup

START_URL = ("https://careers.mediclinic.com/Hirslanden/search/"
             "?createNewAlert=false&q=&optionsFacetsDD_customfield3="
             "&optionsFacetsDD_country=&optionsFacetsDD_customfield5="
             "&optionsFacetsDD_facility=&optionsFacetsDD_shifttype=")

OUTFILE = "hirslanden_jobs.json"
POLITE_DELAY = 0.5  # seconds


@dataclass
class Job:
    title: str
    url: str
    facility: Optional[str] = None
    city: Optional[str] = None
    job_id: Optional[str] = None


def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, timeout=25, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def parse_total_and_window(soup: BeautifulSoup) -> tuple[Optional[int], Optional[int]]:
    """
    Parse "Showing 1 to 50 of 334 Jobs" -> (334, 50).
    Returns (total, window_size).
    """
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+Jobs", text, re.I)
    if not m:
        return None, None
    a, b, total = map(int, m.groups())
    window = b - a + 1 if b >= a else None
    return total, window


def extract_next_url(base_url: str, soup: BeautifulSoup) -> Optional[str]:
    """
    Find the actual "More Search Results" / next-page link the site renders.
    """
    # Common patterns in SuccessFactors skins:
    sel = (
        "a#next, a#searchMoreJobResults, "
        "a[aria-label*='More Search Results' i], "
        "a:-soup-contains('More Search Results')"
    )
    a = soup.select_one(sel)
    if a and a.get("href"):
        return urljoin(base_url, a["href"])

    # Some templates put a data-nexturl on the anchor
    a = soup.select_one("a[data-nexturl]")
    if a and a.get("data-nexturl"):
        return urljoin(base_url, a["data-nexturl"])

    # Fallback: scan inline scripts for a URL containing startrow=NNN
    for sc in soup.find_all("script"):
        txt = sc.string or ""
        m = re.search(r"""["'](?P<u>[^"']+/search/\?[^"']*startrow=\d+[^"']*)["']""", txt)
        if m:
            return urljoin(base_url, m.group("u"))

    return None


def extract_jobs(base_url: str, soup: BeautifulSoup) -> list[Job]:
    jobs: list[Job] = []

    # Primary: accessible list items
    cards = soup.select("li[role='listitem']")
    # Fallbacks
    if not cards:
        cards = soup.select("div[class*='job-result'], div[class*='jobsearch'], li.job, div.job")

    for c in cards:
        # Title + URL
        a = c.select_one("a[href*='/Hirslanden/']") or \
            c.select_one("a[href*='/job/'], a[aria-label*='Title' i], a.jobTitle-link, a.title")
        if not a or not a.get("href"):
            continue
        title = a.get_text(strip=True)
        url = urljoin(base_url, a["href"])

        # Facility / City — try labelled layout first
        def labelled(label: str) -> Optional[str]:
            lab = c.find(string=lambda s: isinstance(s, str) and s.strip().lower() == label.lower())
            if lab and lab.parent:
                sib = lab.parent.find_next_sibling()
                if sib:
                    return sib.get_text(strip=True) or None
            return None

        facility = labelled("Facility") or labelled("Klinik") or labelled("Einrichtung")
        city = labelled("City") or labelled("Ort")

        if not city:
            node = c.select_one(".jobLocation, .location, [data-automation='job-city']")
            if node:
                city = node.get_text(strip=True) or None
        if not facility:
            node = c.select_one(".facility, [data-automation='job-facility']")
            if node:
                facility = node.get_text(strip=True) or None

        # Derive job_id from URL (numeric or req* segment)
        job_id = None
        try:
            parts = [p for p in urlparse(url).path.split("/") if p]
            for p in reversed(parts):
                if p.lower().startswith("req") or p.isdigit():
                    job_id = p
                    break
        except Exception:
            pass

        if title and url:
            jobs.append(Job(title=title, url=url, facility=facility, city=city, job_id=job_id))

    return jobs


def dedupe(jobs: list[Job]) -> list[Job]:
    seen = set()
    out: list[Job] = []
    for j in jobs:
        key = (j.title, j.url)
        if key not in seen:
            seen.add(key)
            out.append(j)
    return out


def update_query(url: str, **params) -> str:
    """
    Return url with updated query parameters (e.g., startrow=...).
    """
    u = urlparse(url)
    q = parse_qs(u.query, keep_blank_values=True)
    for k, v in params.items():
        q[k] = [str(v)]
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))


def main():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; HirslandenScraper/2.0)",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
    })

    all_jobs: list[Job] = []
    visited_pages = set()

    # 1) Load first page
    url = START_URL
    soup = get_soup(session, url)
    total, window = parse_total_and_window(soup)
    if total:
        print(f"[info] Site reports total={total}, page_window={window or '?'}")

    # 2) Prefer following the *real* next link
    page_idx = 1
    while True:
        print(f"[page {page_idx}] {url}")
        if url in visited_pages:
            break
        visited_pages.add(url)

        # Extract current jobs
        jobs = extract_jobs(url, soup)
        print(f"  -> found {len(jobs)} jobs on this page")
        all_jobs.extend(jobs)

        # Try to find the real "next" URL
        next_url = extract_next_url(url, soup)
        if not next_url:
            break

        # Stop if we already have all
        if total and len(all_jobs) >= total:
            break

        # Load next page
        time.sleep(POLITE_DELAY)
        soup = get_soup(session, next_url)
        url = next_url
        page_idx += 1

    # 3) If following next links didn’t reach total, fallback to startrow iteration
    all_jobs = dedupe(all_jobs)
    if total and len(all_jobs) < total:
        print(f"[fallback] have {len(all_jobs)} < total {total}; trying startrow loop")

        # Detect per-page from banner if available, else guess 50
        per_page = window or 50

        # Start from the next startrow after what we already collected
        # (roughly align to per_page boundary)
        startrow = (len(all_jobs) // per_page) * per_page
        if startrow < len(all_jobs):
            startrow += per_page

        # Iterate until we reach total or no new items returned N times
        empty_runs = 0
        while (not total) or (len(all_jobs) < total):
            page_url = update_query(START_URL, startrow=startrow)
            print(f"[startrow={startrow}] {page_url}")

            soup = get_soup(session, page_url)
            pg_total, pg_window = parse_total_and_window(soup)
            if pg_total and not total:
                total = pg_total
            if pg_window and pg_window != per_page:
                per_page = pg_window  # adapt if site changes per-page

            jobs = extract_jobs(page_url, soup)
            if not jobs:
                empty_runs += 1
                if empty_runs >= 2:
                    print("[fallback] no more jobs returned; stopping")
                    break
            else:
                empty_runs = 0
                print(f"  -> found {len(jobs)} jobs")
                all_jobs.extend(jobs)
                all_jobs = dedupe(all_jobs)

            if total and len(all_jobs) >= total:
                break

            startrow += per_page
            time.sleep(POLITE_DELAY)

    all_jobs = dedupe(all_jobs)

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump([asdict(j) for j in all_jobs], f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(all_jobs)} jobs to {OUTFILE}")
    if total:
        if len(all_jobs) == total:
            print(f"✔ Count matches site total ({total}).")
        else:
            print(f"ℹ Found {len(all_jobs)} jobs; site total said {total}.")


if __name__ == "__main__":
    main()
