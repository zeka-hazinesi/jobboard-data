#!/usr/bin/env python3
# helsana_fixed.py — robust stop conditions (4 pages etc.)
import json, re, time, random, logging, hashlib
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Set
from collections import deque
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Tag

START_URL   = "https://jobs.helsana.ch/?lang=de"
OUTPUT      = "helsana_jobs.json"

BASE_DELAY  = 0.7
MAX_RETRIES = 6
TIMEOUT_S   = 30
DEFAULT_STEP_ITEMS = 12

SEND_PAG_RE  = re.compile(r"sendPagination\((\d+)\)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("helsana")

@dataclass
class Job:
    title: str
    location: Optional[str]
    teaser_url: str
    source_offset: int

def txt(n: Optional[Tag]) -> str:
    return (n.get_text(" ", strip=True) if n else "").replace("\xa0", " ").strip()

def discover_step_items(html: str) -> int:
    m = SEND_PAG_RE.search(html)
    if m:
        try:
            v = int(m.group(1))
            if 1 <= v <= 200: return v
        except ValueError: pass
    return DEFAULT_STEP_ITEMS

def discover_offsets_in_html(html: str) -> List[int]:
    offs = {0}
    offs.update(int(m.group(1)) for m in SEND_PAG_RE.finditer(html))
    return sorted(offs)

def parse_teasers(html: str, source_offset: int) -> List[Job]:
    soup = BeautifulSoup(html, "lxml")
    jobs: List[Job] = []
    seen_local: Set[str] = set()

    # Prefer anchors that look like detail pages on the same domain
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if href.startswith("#"): continue
        absu = urljoin(START_URL, href)
        p = urlparse(absu)
        if "jobs.helsana.ch" not in p.netloc: continue
        if "/offene-stellen/" not in p.path and "/job/" not in p.path: continue

        if absu in seen_local: continue
        seen_local.add(absu)

        title = (a.get("title") or "").strip() or txt(a)
        if not title:
            # try heading nearby
            parent = a
            for _ in range(4):
                parent = parent.parent or parent
                h = parent.find(["h2","h3"])
                if h: title = txt(h); break
        if not title:
            title = p.path.rstrip("/").split("/")[-1].replace("-", " ").title()

        # quick location guess from a nearby meta block
        location = ""
        parent = a
        for _ in range(4):
            parent = parent.parent or parent
            if not isinstance(parent, Tag): break
            meta = parent.select_one(".c-teaser__meta, .meta, .job-meta, .key-value, .c-key-value")
            if meta:
                mt = txt(meta)
                if mt and len(mt) <= 160:
                    location = mt
                    break

        jobs.append(Job(title=title, location=location or None, teaser_url=absu, source_offset=source_offset))
    return jobs

def polite_post(client: httpx.Client, url: str, data: Dict[str,str], page_no: int, offset: int) -> str:
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        log.info("→ Request PAGE %d (offset=%d), attempt %d …", page_no, offset, attempt)
        time.sleep(delay + random.uniform(0, 0.25))
        r = client.post(url, data=data)
        if r.status_code in (429, 503):
            ra = r.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else min(10.0, delay * 2.0 + random.uniform(0, 0.6))
            log.warning("Rate limited (%s) — sleeping %.2fs", r.status_code, wait)
            time.sleep(wait)
            delay = min(6.0, delay * 1.8)
            continue
        r.raise_for_status()
        return r.text
    raise RuntimeError("Max retries reached.")

def main() -> int:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; HelsanaSerialScraper/1.1)",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": START_URL,
        "Origin": "https://jobs.helsana.ch",
        "X-Requested-With": "XMLHttpRequest",
    }
    with httpx.Client(headers=headers, follow_redirects=True, timeout=TIMEOUT_S) as client:
        # Landing: get form + page size
        log.info("Loading landing: %s", START_URL)
        landing = client.get(START_URL)
        landing.raise_for_status()
        step_items = discover_step_items(landing.text)
        log.info("Discovered page size: %d", step_items)

        # Build base payload exactly as form expects
        soup0 = BeautifulSoup(landing.text, "lxml")
        form = soup0.find("form")
        if not form:
            raise RuntimeError("No <form> on landing.")
        action = urljoin(START_URL, form.get("action") or START_URL)
        payload_base = {
            "offset": "0",
            "limit": str(step_items),
            "lang": "de",
            "cv_ref": "",
            "query": "",
            "filter_10": "",
            "filter_20": "",
            "filter_30": "",
        }
        # take any hidden inputs from form as well
        for inp in form.find_all("input"):
            name = inp.get("name"); val = inp.get("value") or ""
            if name and name not in payload_base:
                payload_base[name] = val

        # Offset queue discovered from landing + each response
        queue: deque[int] = deque(discover_offsets_in_html(landing.text))
        log.info("Initial offsets: %s", list(queue))

        seen_offsets: Set[int] = set()
        seen_hashes: Set[str] = set()
        seen_urls: Set[str] = set()
        results: List[Job] = []

        while queue:
            off = queue.popleft()
            if off in seen_offsets:
                continue
            seen_offsets.add(off)
            page_no = off // step_items + 1

            payload = dict(payload_base); payload["offset"] = str(off)
            html = polite_post(client, action, payload, page_no, off)

            # stop if we got an identical page we’ve seen already
            h = hashlib.sha256(html.encode("utf-8", "ignore")).hexdigest()
            if h in seen_hashes:
                log.info("← PAGE %d offset=%d: identical HTML — stopping.", page_no, off)
                break
            seen_hashes.add(h)

            # parse jobs
            jobs_here = parse_teasers(html, off)
            new_here = 0
            for j in jobs_here:
                if j.teaser_url in seen_urls:
                    continue
                seen_urls.add(j.teaser_url)
                results.append(j)
                new_here += 1
            log.info("← PAGE %d offset=%d: %d job(s), %d new (total %d)",
                     page_no, off, len(jobs_here), new_here, len(results))

            # If this page added nothing new, we likely reached the end
            if new_here == 0:
                log.info("No new jobs on this page — stopping.")
                break

            # discover more offsets from this response
            newly = [n for n in discover_offsets_in_html(html) if n not in seen_offsets and n not in queue]
            if newly:
                log.info("Discovered more offsets from PAGE %d: %s", page_no, newly)
                for n in newly:
                    queue.append(n)

            # also push the next sequential offset (off + step) if not already queued
            nxt = off + step_items
            if nxt not in seen_offsets and nxt not in queue:
                queue.append(nxt)

        data = [asdict(j) for j in results]

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info("Done. Pages visited: %d | Jobs: %d", len(seen_offsets), len(data))
    print(f"Wrote {OUTPUT} with {len(data)} jobs. Offsets seen: {sorted(seen_offsets)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
