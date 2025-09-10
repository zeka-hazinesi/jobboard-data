#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, re, sys, time
from dataclasses import asdict, dataclass
from typing import List, Dict, Set, Optional
import requests
from bs4 import BeautifulSoup

BASE = "https://jobs.h-och.ch/search/"
PARAMS_BASE = {
    "q": "",
    "sortColumn": "referencedate",
    "sortDirection": "desc",
    "startrow": 0,  # will be updated in loop
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) PythonScraper/1.0 (+https://example.org)",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Regex für „Ergebnisse 1 – 25 von 212“ (de) oder „Results 1 – 25 of 212“ (en)
TOTAL_RX = re.compile(
    r"(?:Ergebnisse|Results)\s+\d+\s*[–-]\s*\d+\s*(?:von|of)\s*(\d+)",
    re.IGNORECASE,
)

# Hilfs-Selektoren (verschiedene Taleo/Oracle-Layouts)
TITLE_SELECTORS = [
    "a.jobTitle-link",         # häufig
    "a[href*='/job/']",        # fallback
    "a[href*='/jobs/']",
    "td.colTitle a",           # tabellarisch
    "article a",               # karten
]
ROW_SELECTORS = [
    "tr.data-row",             # Tabelle
    "tr[id^='job-']",          # Tabelle (ID)
    "div.job-tile",            # Karten
    "article",                 # Karten
    "li.jobs-list-item",       # Liste
]


@dataclass
class Job:
    title: str
    url: str
    location: Optional[str] = None
    date: Optional[str] = None
    req_id: Optional[str] = None
    department: Optional[str] = None
    company: str = "H-OCH"
    source: str = BASE

def clean_text(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    t = re.sub(r"\s+", " ", x).strip()
    return t or None

def parse_total(soup: BeautifulSoup) -> Optional[int]:
    # Suche in den üblichen Stellen
    candidates = []
    for sel in ["#searchCount", ".searchResultsCount", ".pagination-label", "body"]:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if txt:
                candidates.append(txt)
    joined = " | ".join(candidates)
    m = TOTAL_RX.search(joined)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None

def extract_text_near(el, labels: List[str]) -> Optional[str]:
    # durchsucht nahe Umgebung nach Labeln wie "Standort"/"Ort"/"Location", "Datum", "Requisition"
    ctx = el.get_text(" ", strip=True)
    for label in labels:
        m = re.search(label + r"\s*[:]\s*([^\|·•]+)", ctx, flags=re.IGNORECASE)
        if m:
            return clean_text(m.group(1))
    return None

def parse_row(row) -> Optional[Job]:
    # Titel + URL
    a = None
    for sel in TITLE_SELECTORS:
        a = row.select_one(sel)
        if a and a.get_text(strip=True):
            break
    if not a:
        # manchmal liegt der Link im gesamten row
        a = row if row.name == "a" else row.find("a")
    if not a or not a.get_text(strip=True):
        return None

    title = clean_text(a.get_text(strip=True))
    href = a.get("href") or ""
    if href and href.startswith("/"):
        url = "https://jobs.h-och.ch" + href
    elif href.startswith("http"):
        url = href
    else:
        url = "https://jobs.h-och.ch" + "/" + href.lstrip("/")

    # Weitere Felder heuristisch
    # Location
    loc = None
    for sel in [".jobLocation", ".colLocation", ".location", "[data-qa='location']"]:
        el = row.select_one(sel)
        if el:
            loc = clean_text(el.get_text())
            break
    if not loc:
        loc = extract_text_near(row, ["Standort", "Ort", "Location"])

    # Datum
    date = None
    for sel in [".jobDate", ".colDate", ".posting-date", "[data-qa='posting-date']"]:
        el = row.select_one(sel)
        if el:
            date = clean_text(el.get_text())
            break
    if not date:
        date = extract_text_near(row, ["Datum", "Date", "Veröffentlicht", "Posting"])

    # Req-ID / Kennziffer
    req_id = extract_text_near(row, ["Kennziffer", "Req", "Requisition", "Job ID", "Stellen-Nr"])

    # Abteilung
    dept = None
    for sel in [".category", ".department", "[data-qa='department']"]:
        el = row.select_one(sel)
        if el:
            dept = clean_text(el.get_text())
            break

    return Job(title=title or "", url=url, location=loc, date=date, req_id=req_id, department=dept)

def parse_jobs(html: str) -> List[Job]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Job] = []
    # Finde total (nur fürs Logging)
    # (nicht kritisch, wird in main() erneut versucht)
    for row_sel in ROW_SELECTORS:
        for row in soup.select(row_sel):
            job = parse_row(row)
            if job:
                jobs.append(job)

    # Falls obige Selektoren nichts ergaben, versuche „alle a“-Fallbacks
    if not jobs:
        for a in soup.find_all("a", href=True):
            if re.search(r"/job/", a["href"]):
                title = clean_text(a.get_text())
                if not title:
                    continue
                href = a["href"]
                url = href if href.startswith("http") else "https://jobs.h-och.ch" + href
                jobs.append(Job(title=title, url=url))
    return jobs

def fetch_page(sess: requests.Session, startrow: int) -> str:
    params = dict(PARAMS_BASE)
    params["startrow"] = startrow
    r = sess.get(BASE, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.text

def main():
    out_file = "h_och_jobs.json"
    step = 25
    collected: List[Dict] = []
    seen_urls: Set[str] = set()
    total_expected: Optional[int] = None

    with requests.Session() as sess:
        startrow = 0
        page_idx = 1
        while True:
            html = fetch_page(sess, startrow)
            soup = BeautifulSoup(html, "html.parser")
            if total_expected is None:
                total_expected = parse_total(soup)

            page_jobs = parse_jobs(html)
            # Deduplizieren
            new_count = 0
            for j in page_jobs:
                if j.url not in seen_urls:
                    seen_urls.add(j.url)
                    collected.append(asdict(j))
                    new_count += 1

            print(f"[Page {page_idx:>2} startrow={startrow}] found={len(page_jobs)} new={new_count} total={len(collected)}"
                  + (f" (expected≈{total_expected})" if total_expected else ""))

            # Abbruchbedingungen
            if new_count == 0:
                # keine neuen Jobs -> Ende
                break
            if total_expected and len(collected) >= total_expected:
                # genug gesammelt
                break

            page_idx += 1
            startrow += step
            time.sleep(0.8)  # sanftes Throttling

    # Ergebnis speichern
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(collected)} jobs to {out_file}"
          + (f" (site total said {total_expected})" if total_expected else ""))

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        sys.stderr.write(f"HTTPError: {e}\nResponse text (truncated):\n{getattr(e.response,'text','')[:500]}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")
        sys.exit(1)
