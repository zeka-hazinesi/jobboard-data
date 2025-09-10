#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = "https://apply.refline.ch/792841/search.html"
OUTFILE = Path("zkb_jobs.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET", "HEAD"},
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def find_jobs_table(soup: BeautifulSoup):
    """
    Try common patterns used on Refline pages:
    - table with class 'result' or 'jobs' or similar
    - otherwise, the first table containing job links
    """
    # Likely candidates by class
    for cls in ["result", "jobs", "joblist", "table"]:
        t = soup.select_one(f"table.{cls}")
        if t:
            return t

    # Fallback: first table that has at least one link to a detail page
    for t in soup.find_all("table"):
        if t.select('a[href*="/792841/"]') or t.select('a[href*="/vacancies/"]') or t.select("a[href*='.html']"):
            return t
    # Last fallback: first table in the document
    return soup.find("table")

def table_to_json(table, base_url: str) -> list[dict]:
    # headers: prefer <th>, fallback to first row's <td>
    headers = []
    thead = table.find("thead")
    if thead:
        ths = thead.find_all(["th", "td"])
        headers = [clean(th.get_text(" ", strip=True)) for th in ths]

    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [clean(td.get_text(" ", strip=True)) for td in first_row.find_all(["th", "td"])]

    # normalize header keys
    keys = []
    used = set()
    for h in headers:
        key = re.sub(r"[^A-Za-z0-9]+", "_", h).strip("_").lower() or "col"
        # ensure uniqueness
        i = 2
        base = key
        while key in used:
            key = f"{base}_{i}"
            i += 1
        used.add(key)
        keys.append(key)

    # choose data rows (skip header row if it's in <tbody>, detect by tag names)
    body_rows = []
    tbody = table.find("tbody")
    if tbody:
        body_rows = tbody.find_all("tr", recursive=False) or tbody.find_all("tr")
    if not body_rows:
        # fallback: all rows except the first header-like row
        all_rows = table.find_all("tr")
        body_rows = all_rows[1:] if len(all_rows) > 1 else all_rows

    records = []
    for tr in body_rows:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        row = {}
        for i, td in enumerate(tds):
            key = keys[i] if i < len(keys) else f"col_{i+1}"
            text = clean(td.get_text(" ", strip=True))

            # if the cell has a main link, capture link text + absolute URL
            a = td.find("a", href=True)
            if a:
                link_text = clean(a.get_text(" ", strip=True)) or text
                href = urljoin(base_url, a["href"])
                row[key] = link_text
                # append a parallel URL field
                row[f"{key}_url"] = href
            else:
                row[key] = text

        records.append(row)

    return records

def main():
    sess = make_session()
    resp = sess.get(URL, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = find_jobs_table(soup)
    if not table:
        raise SystemExit("✗ Could not find a jobs table on the page. The structure may have changed.")

    data = table_to_json(table, URL)
    OUTFILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ Extracted {len(data)} rows → {OUTFILE}")

if __name__ == "__main__":
    main()
