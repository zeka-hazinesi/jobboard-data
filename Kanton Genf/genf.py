#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.ge.ch/offres-emploi-etat-geneve/liste-offres"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36"
}

REMU_RE = re.compile(r"\bclasse\s+\d+\b|\bEn cours\b", re.I)
RATE_RE = re.compile(r"\b(\d{1,3}\s?(?:à\s)?\s?\d{0,3})\s?%\b|\b\d{1,3}%\b", re.I)

def fetch_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def extract_jobs(soup: BeautifulSoup) -> List[Dict]:
    """
    The overview page lists offers as bullet items:
      * <a>Title</a>
        <dept/org line>
        Rémunération
        <classe X>
        Taux d'activité
        <rate>

    We’ll walk the content and, for each job link (href contains '/liste-offres/'),
    gather nearby text until the next job link to collect department, remuneration, and rate.
    """
    jobs: List[Dict] = []

    # Narrow to the main content area that contains the list (keeps parsing stable)
    main = soup
    # Find all anchors that look like offer titles
    anchors: List[Tag] = [
        a for a in main.find_all("a", href=True)
        if "/offres-emploi-etat-geneve/liste-offres/" in a["href"]
    ]

    seen = set()
    for i, a in enumerate(anchors):
        # De-dup: the same anchor might appear in TOC or elsewhere
        href = requests.compat.urljoin(BASE_URL, a["href"])
        title = " ".join(a.get_text(" ", strip=True).split())
        if not title or href in seen:
            continue

        # Collect sibling text until the next job anchor
        # Strategy: iterate through the next siblings in the DOM until we bump into
        # another anchor that also looks like a job title (or until parent section ends).
        texts: List[str] = []
        node = a.parent
        # Ensure we start scanning after the anchor’s container if it is within a list item
        # so we capture the block following it.
        start = a
        # Advance step-by-step in document order
        cur = start.next_sibling
        next_href = anchors[i + 1]["href"] if i + 1 < len(anchors) else None

        # To cap our scan, we stop once we hit the next anchor (by id or href match), or
        # a new list item beginning with another offer’s anchor.
        while cur:
            if isinstance(cur, Tag):
                # If we encounter another job link, stop
                links = cur.find_all("a", href=True)
                if links:
                    if any("/offres-emploi-etat-geneve/liste-offres/" in l["href"] for l in links):
                        break
                # Otherwise, collect visible text
                t = cur.get_text(" ", strip=True)
                if t:
                    texts.append(t)
            else:
                # NavigableString
                t = str(cur).strip()
                if t:
                    texts.append(t)
            cur = cur.next_sibling

        block = " ".join(texts).strip()

        # Heuristics to extract fields from the block
        # Department/office: first sentence/line that’s not the “Rémunération / Taux …” labels
        department = None
        remuneration = None
        activity_rate = None

        # Split into pseudo-lines to stabilize parsing
        parts = [p.strip() for p in re.split(r"\s{2,}|\n| {3,}", block) if p.strip()]
        # 1) Find a reasonable department/office line:
        for p in parts:
            if p.lower().startswith("rémunération") or p.lower().startswith("taux d'activité"):
                continue
            # Often looks like "Département ... / Office ..."
            if " / " in p or "Département" in p or "Pouvoir judiciaire" in p or "Autres" in p:
                department = p
                break

        # 2) Remuneration (e.g., "classe 21" or "En cours")
        # The page sometimes places "Rémunération" label and then the class next.
        m = REMU_RE.search(" ".join(parts))
        if m:
            remuneration = m.group(0)

        # 3) Activity rate, e.g., "80%" or "80 à 100%"
        m2 = RATE_RE.search(" ".join(parts))
        if m2:
            # normalize spaces
            activity_rate = m2.group(0).replace("  ", " ").strip()

        jobs.append({
            "title": title or None,
            "url": href,
            "department": department,
            "remuneration": remuneration,
            "activity_rate": activity_rate
        })
        seen.add(href)

    return jobs

def main():
    out_file = "ge_geneva_jobs.json"
    soup = fetch_soup(BASE_URL)
    jobs = extract_jobs(soup)

    # Optional: lightweight de-dup by URL
    uniq = {j["url"]: j for j in jobs}
    jobs = list(uniq.values())

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)

    print(f"[✓] Saved {len(jobs)} jobs to {out_file}")

if __name__ == "__main__":
    main()
