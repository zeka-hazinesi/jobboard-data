#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup, NavigableString, Tag

BASE_URL = "https://implenia.com/karriere/jobs/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}

# Regex helpers
TOTAL_RE = re.compile(r"\b(\d+)\s+Stellen\b", re.I)
JOBID_RE = re.compile(r"\bJob\s+(\d+)\b", re.I)

def fetch_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def get_total_count(soup: BeautifulSoup) -> Optional[int]:
    # The page shows e.g. "108 Stellen"
    text = soup.get_text(" ", strip=True)
    m = TOTAL_RE.search(text)
    return int(m.group(1)) if m else None

def text_between(h3: Tag, next_h3: Optional[Tag]) -> str:
    """Collect plaintext between this h3 and the next h3 (same listing group)."""
    parts: List[str] = []
    node = h3.next_sibling
    while node and node is not next_h3:
        if isinstance(node, NavigableString):
            s = str(node).strip()
            if s:
                parts.append(s)
        elif isinstance(node, Tag):
            # Keep headings, paragraphs and small meta blocks
            if node.name in {"h4", "p", "div", "span", "small"}:
                s = node.get_text(" ", strip=True)
                if s:
                    parts.append(s)
        node = node.next_sibling
    return " \n ".join(parts).strip()

def parse_listing_group(h3: Tag) -> Optional[Dict]:
    # Title & URL
    a = h3.find("a", href=True)
    if not a or "/karriere/job/" not in a["href"]:
        return None
    title = " ".join(a.get_text(" ", strip=True).split())
    url = requests.compat.urljoin(BASE_URL, a["href"])

    # Find the next h3 (marks the next listing), then gather the segment in between
    next_h3 = h3.find_next("h3")
    seg = text_between(h3, next_h3)

    # Location heuristic: first line that looks like "City (CH)" etc. (overview uses h4 for this)
    location = None
    for line in [x.strip() for x in seg.split("\n") if x.strip()]:
        # Location lines often end with "(CH)", "(DE)", "(AT)", "(SE)", "(NO)", "(FR)"
        if re.search(r"\(([A-Z]{2})\)$", line):
            location = line
            break

    # Meta line with category, company and "Job ####"
    job_id = None
    company = None
    category = None

    # Try to find the line that contains "Job ####"
    meta_line = None
    for line in [x.strip() for x in seg.split("\n") if x.strip()]:
        if "Job" in line:
            meta_line = line
            break
    if meta_line:
        m = JOBID_RE.search(meta_line)
        if m:
            job_id = m.group(1)
        # Company often right before "Job ####", e.g. "Implenia Schweiz AG", "Wincasa AG"
        # Category usually at the beginning of the line.
        # Example: "Bauleitung / Bauführung / Projektleitung Implenia Schweiz AG Job 11780"
        tokens = meta_line.split()
        try:
            jpos = tokens.index("Job")
            # Company is tokens before "Job", after removing a leading category chunk.
            # We'll split the meta_line by " Job " and then try a simple split from the right.
            left = meta_line.rsplit(" Job ", 1)[0]
            # Heuristic: the company is the last two or three words before Job
            # but since company names vary, take the tail that matches "* AG" or looks like a company
            mcmp = re.search(r"([A-ZÄÖÜ][\w\.\-]*(?:\s+[A-ZÄÖÜ][\w\.\-]*)*\s+AG)$", left)
            if mcmp:
                company = mcmp.group(1).strip()
                category = left[: left.rfind(company)].strip(" –-")
            else:
                # fallback: last 2 words as company
                pieces = left.split()
                if len(pieces) >= 2:
                    company = " ".join(pieces[-2:])
                    category = " ".join(pieces[:-2]).strip(" –-") or None
        except ValueError:
            pass

    # Short snippet: take the first paragraph-ish sentence after location
    snippet = None
    # Try to capture the first p-text from the segment after the (detected) location
    lines = [x.strip() for x in seg.split("\n") if x.strip()]
    if lines:
        # Remove the location & meta line to pick a descriptive line
        filtered = [ln for ln in lines if ln not in {location or "", meta_line or ""}]
        if filtered:
            snippet = filtered[0]

    return {
        "title": title or None,
        "url": url,
        "location": location,
        "company": company,
        "category": category,
        "job_id": job_id,
        "snippet": snippet
    }

def extract_jobs(soup: BeautifulSoup) -> List[Dict]:
    jobs: List[Dict] = []
    seen_urls = set()
    # Each listing’s title is an <h3> containing a link to /karriere/job/<id>/
    for h3 in soup.find_all("h3"):
        # Skip non-job headings
        a = h3.find("a", href=True)
        if not a or "/karriere/job/" not in (a["href"] or ""):
            continue
        item = parse_listing_group(h3)
        if item and item["url"] not in seen_urls:
            jobs.append(item)
            seen_urls.add(item["url"])
    return jobs

def main():
    out_file = "implenia_jobs.json"
    soup = fetch_soup(BASE_URL)

    total = get_total_count(soup)
    print(f"[i] Total shown on page: {total if total is not None else 'N/A'}")

    jobs = extract_jobs(soup)
    print(f"[✓] Extracted {len(jobs)} jobs from overview")

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    print(f"[✓] Saved to {out_file}")

if __name__ == "__main__":
    main()
