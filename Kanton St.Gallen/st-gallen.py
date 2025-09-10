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

BASE = "https://recruitingapp-2800.umantis.com/Jobs/All"
OUTFILE = Path("st_gallen_jobs.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET", "HEAD"}
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def extract_job_id(url: str) -> str | None:
    if not url:
        return None
    m = re.search(r"/Vacancies/(\d+)", url)
    return m.group(1) if m else None

def parse_job_anchor(a, base_url: str) -> dict:
    title = clean(a.get_text(" ", strip=True))
    href = a.get("href", "").strip()
    url = urljoin(base_url, href)

    # Walk up a bit and gather nearby text to heuristically fetch metadata
    container = a
    for _ in range(3):
        if container.parent:
            container = container.parent
    nearby = " ".join(
        clean(el.get_text(" ", strip=True))
        for el in container.find_all(["div", "span", "li", "td", "p"], recursive=True)[:24]
    )

    # Try to pull fields like "Art: Vollzeit", department, and location that often appear inline
    m_art = re.search(r"\bArt:\s*([^|•\n\r]+)", nearby, flags=re.I)
    m_dep = re.search(r"(?:Departement|Department)\s*:\s*([^|•\n\r]+)", nearby, flags=re.I)
    m_loc = re.search(r"(?:Ort|Standort)\s*:\s*([^|•\n\r]+)", nearby, flags=re.I)

    return {
        "id": extract_job_id(url),
        "title": title or None,
        "url": url,
        "employment_type": clean(m_art.group(1)) if m_art else None,
        "department": clean(m_dep.group(1)) if m_dep else None,
        "location": clean(m_loc.group(1)) if m_loc else None,
        "source_context": nearby[:240] or None,
    }

def extract_jobs_from_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # This Umantis uses /Vacancies/<id>/Description/<lang> links for jobs
    anchors = soup.select('a[href*="/Vacancies/"][href*="/Description"]')

    # Fallback: any <a> that looks like a job (title text + /Vacancies/)
    if not anchors:
        anchors = [a for a in soup.find_all("a") if "/Vacancies/" in (a.get("href") or "")]

    # Filter meaningful anchors
    seen = set()
    picked = []
    for a in anchors:
        href = (a.get("href") or "").strip()
        txt = clean(a.get_text(" ", strip=True))
        if not href or not txt:
            continue
        key = (href, txt)
        if key in seen:
            continue
        seen.add(key)
        picked.append(a)

    return [parse_job_anchor(a, page_url) for a in picked]

def fetch_page(sess: requests.Session, page_num: int) -> tuple[str, str]:
    # p1..p4
    url = f"{BASE}?tc1152481=p{page_num}"
    r = sess.get(url, timeout=30)
    r.raise_for_status()
    return url, r.text

def main():
    sess = make_session()
    all_by_url = {}

    for p in range(1, 5):
        page_url, html = fetch_page(sess, p)
        jobs = extract_jobs_from_page(html, page_url)
        print(f"[+] p{p}: {len(jobs)} job links")
        for j in jobs:
            all_by_url[j["url"]] = j  # de-dupe across pages

    jobs = list(all_by_url.values())
    jobs.sort(key=lambda x: (x.get("id") or "", x.get("title") or ""))

    OUTFILE.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[✓] Saved {len(jobs)} jobs to {OUTFILE}")

if __name__ == "__main__":
    main()
