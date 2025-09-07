#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Rolex job listings (no details) into rolex_jobs.json.

Usage:
  python rolex_scraper.py
"""

import json
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://www.carrieres-rolex.com"
LISTING_TMPL = (
    BASE
    + "/Rolex/go/Toutes-nos-offres-Rolex/2901501/{offset}/?q=&sortColumn=referencedate&sortDirection=desc"
)
STEP = 25
OUTFILE = "rolex_jobs.json"
SLEEP_SECONDS = 0.7
TIMEOUT = 20

# Known vocab on the site to help extract columns without opening detail pages
DOMAINS = {
    "Métiers horlogers",
    "Fabrication",
    "Ingénierie / R&D",
    "Commercial",
    "Communication",
    "Création / Design",
    "Finance / Audit",
    "Qualité",
    "Ressources humaines",
    "Secrétariat / Administration",
    "Bâtiments & Infrastructures / Sécurité",
    "Achat / Chaîne logistique",
    "Systèmes d’information",
    "Autres domaines",
    "Services généraux / Sécurité",  # appears in rows
    "Ingénierie / R & D",            # variant spacing in rows
}
SITES = {"Genève", "Bienne", "Fribourg"}
CONTRACTS = {"CDI", "CDD", "Apprentissage", "Stage", "Stage découverte"}

DETAIL_HREF_RE = re.compile(r"/Rolex/job/.+?/\d+/?$")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0; +https://example.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

def fetch_html(url: str) -> str:
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"[rate-limit] 429 on {url} — backing off {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if attempt == 2:
                raise
            wait = 2 ** attempt
            print(f"[warn] {e} — retrying in {wait}s")
            time.sleep(wait)
    return ""

def nearest_row(tag):
    """
    Given an <a> tag, try to find its row container (<tr>, or a div/li that holds the row).
    """
    cur = tag
    for _ in range(6):
        if cur is None:
            break
        if cur.name in ("tr", "li", "article"):
            return cur
        # Common grid rows are often plain divs; accept div if it has other texts
        if cur.name == "div" and len(list(cur.stripped_strings)) > 1:
            return cur
        cur = cur.parent
    return tag.parent  # fallback

def extract_columns_from_row(row_text: str, title: str):
    """
    From the row text (which often looks like: '<title> <title> <domain> <site> <contract>'),
    try to extract (domain, site, contract).
    """
    # Remove title occurrences to reduce noise
    clean = row_text.replace(title, " ").strip()
    # Normalize spacing and weird non-breaking spaces
    clean = re.sub(r"\s+", " ", clean).replace("\xa0", " ").strip()

    found_domain = next((d for d in DOMAINS if d in clean), None)
    found_site = next((s for s in SITES if re.search(rf"\b{s}\b", clean)), None)
    # 'Stage découverte' must be checked before 'Stage'
    found_contract = None
    for c in sorted(CONTRACTS, key=len, reverse=True):
        if c in clean:
            found_contract = c
            break

    return found_domain, found_site, found_contract

def parse_jobs(html: str):
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    seen_hrefs = set()

    # Collect all job title anchors that point to a detail page
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not DETAIL_HREF_RE.search(href):
            continue
        abs_url = urljoin(BASE, href)
        if abs_url in seen_hrefs:
            continue  # duplicates (desktop/mobile variants)
        title = a.get_text(strip=True)
        if not title:
            continue

        row = nearest_row(a)
        row_text = " ".join(row.stripped_strings) if row else title
        domain, site, contract = extract_columns_from_row(row_text, title)

        jobs.append(
            {
                "title": title,
                "url": abs_url,
                **({"domain": domain} if domain else {}),
                **({"site": site} if site else {}),
                **({"contract": contract} if contract else {}),
            }
        )
        seen_hrefs.add(abs_url)

    return jobs

def main():
    all_jobs = []
    offset = 0
    page_idx = 1
    while True:
        url = LISTING_TMPL.format(offset=offset)
        print(f"[page {page_idx}] GET {url}")
        html = fetch_html(url)
        page_jobs = parse_jobs(html)
        count = len(page_jobs)
        print(f"[page {page_idx}] found {count} jobs")
        if count == 0:
            break
        all_jobs.extend(page_jobs)
        offset += STEP
        page_idx += 1
        time.sleep(SLEEP_SECONDS)

    # De-dup across pages (just in case)
    dedup = {}
    for j in all_jobs:
        dedup[j["url"]] = j
    final_list = list(dedup.values())

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(final_list)} jobs to {OUTFILE}")

if __name__ == "__main__":
    main()
