#!/usr/bin/env python3
# fenaco.py — Fenaco jobs (jobs.fenaco.com) scraper
# - Sequential (no parallel requests)
# - Mimics the site's pagination form (offset=0,7,14,…)
# - Discovers new offsets from onclick="sendPagination(N)" on *each* response
# - Logs page/offset progress and explicit rate-limit events (429/503)
# - Parses ONLY teaser cards (no detail-page fetches)

import json, re, time, random, logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Set
from collections import deque
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag

# ---------- Settings ----------
START_URL   = "https://jobs.fenaco.com/"
OUTPUT      = "fenaco_jobs.json"

BASE_DELAY  = 0.9     # base delay between requests (seconds) — increase if still throttled
MAX_RETRIES = 6
TIMEOUT_S   = 30
STEP_ITEMS  = 7       # number of items per page; used for safety stepping after last offset

# ---------- Regex ----------
SEND_PAG_RE  = re.compile(r"sendPagination\((\d+)\)")
WORKLOAD_RE  = re.compile(r"([0-9]{1,3}\s?(?:–|-|to)\s?[0-9]{1,3}%|[0-9]{1,3}%)")
CONTRACT_RE  = re.compile(r"\b(unbefristet|befristet|vollzeit|teilzeit)\b", re.I)

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s"
)
log = logging.getLogger("fenaco")

# ---------- Models ----------
@dataclass
class Job:
    title: str
    company: Optional[str]
    location: Optional[str]
    workload: Optional[str]
    contract: Optional[str]
    teaser_url: str
    source_offset: int

# ---------- Helpers ----------
def txt(node: Optional[Tag]) -> str:
    return (node.get_text(" ", strip=True) if node else "").replace("\xa0", " ").strip()

def abs_url(base: str, href: Optional[str]) -> Optional[str]:
    return urljoin(base, href) if href else None

def find_form_and_payload(html: str, base: str) -> Tuple[str, Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    form = None
    for f in soup.find_all("form"):
        if f.find("input", {"name": "offset"}):
            form = f; break
    if not form:
        raise RuntimeError("Pagination form (input name='offset') not found.")

    action = abs_url(base, form.get("action") or START_URL) or START_URL
    payload: Dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        payload[name] = inp.get("value") or ""

    if "lang" not in payload:
        m = re.search(r'<html[^>]*\blang=["\']([a-zA-Z-]+)["\']', html)
        payload["lang"] = m.group(1) if m else "de"
    return action, payload

def discover_offsets(html: str) -> List[int]:
    offs = {0}
    offs.update(int(m.group(1)) for m in SEND_PAG_RE.finditer(html))
    return sorted(offs)

def parse_teasers(html: str, source_offset: int) -> List[Job]:
    soup = BeautifulSoup(html, "lxml")
    jobs: List[Job] = []
    seen_local: Set[Tuple[str,str]] = set()

    for a in soup.select("a[href*='/offene-stellen/']"):
        href = a.get("href") or ""
        if href.startswith(("mailto:", "tel:")):
            continue
        if href.startswith("/"):
            href = "https://jobs.fenaco.com" + href

        # climb to a card-like wrapper
        card = a
        for _ in range(6):
            if card.parent and card.parent.name in ("article","li","div","section"):
                card = card.parent; break
            card = card.parent or card

        title = txt(card.find(["h2","h3"])) or (a.get("title") or "").strip() or txt(a)
        if not title:
            title = href.rstrip("/").split("/")[-2].replace("-", " ").title()

        block = txt(card)

        company, location = None, None
        for sel in ("li",".chip",".tag",".badge",".company",".location"):
            for node in card.select(sel):
                t = txt(node)
                if not t: continue
                if not company and any(k in t for k in ["fenaco","LANDI","UFA","VOLG","TRAVECO","Provins","Ramseier","Bison"]):
                    company = t
                if not location and re.search(r"[A-ZÄÖÜ][\wÄÖÜäöü.\- ]+(?:\s\([^)]+\))?$", t):
                    location = t
        if not location:
            i = block.find(title)
            after = block[i+len(title):] if i >= 0 else block
            m = re.search(r",\s*([A-ZÄÖÜ][\wÄÖÜäöü.\- ]+)", after)
            if m:
                location = m.group(1).strip()
                company = (after.split(",")[0].strip() or company)

        workload = (WORKLOAD_RE.search(block) or (None,))[0] if WORKLOAD_RE.search(block) else None
        contract = (CONTRACT_RE.search(block) or (None,))[0] if CONTRACT_RE.search(block) else None

        key = (href, title)
        if key in seen_local:
            continue
        seen_local.add(key)
        jobs.append(Job(title, company, location, workload, contract, href, source_offset))
    return jobs

def rate_limit_wait(resp: httpx.Response, attempt: int, delay: float) -> float:
    ra = resp.headers.get("Retry-After")
    if ra and ra.isdigit():
        wait = float(ra)
    else:
        wait = min(10.0, delay * 2.0 + random.uniform(0.0, 0.6))
    log.warning("Rate limited (%s) — honoring backoff: sleeping %.2fs (attempt %d)",
                resp.status_code, wait, attempt)
    time.sleep(wait)
    return min(6.0, delay * 1.8)

def polite_post(client: httpx.Client, url: str, data: Dict[str,str], page_no: int, offset: int) -> str:
    """Sequential POST with logs + retries/backoff."""
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        log.info("→ Request PAGE %d (offset=%d), attempt %d …", page_no, offset, attempt)
        time.sleep(delay + random.uniform(0, 0.25))  # spacing + jitter
        r = client.post(url, data=data)
        if r.status_code in (429, 503):
            log.warning("Received %d for PAGE %d (offset=%d).", r.status_code, page_no, offset)
            delay = rate_limit_wait(r, attempt, delay)
            continue
        try:
            r.raise_for_status()
            return r.text
        except httpx.HTTPStatusError as e:
            log.error("HTTP %d on PAGE %d (offset=%d): %s", r.status_code, page_no, offset, e)
            if attempt == MAX_RETRIES:
                raise
            delay = min(6.0, delay * 1.6)
    raise RuntimeError("Unreachable retry loop")

def main() -> int:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; FenacoSerialScraper/1.2)",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": START_URL,
        "Origin": "https://jobs.fenaco.com",
        "X-Requested-With": "XMLHttpRequest",
    }
    with httpx.Client(headers=headers, follow_redirects=True, timeout=TIMEOUT_S) as client:
        log.info("Loading landing page: %s", START_URL)
        landing = client.get(START_URL)
        landing.raise_for_status()

        action, base_payload = find_form_and_payload(landing.text, START_URL)
        log.info("Detected form action: %s", action)

        # Initial offsets from landing (e.g., 0,7,14)
        queue = deque(discover_offsets(landing.text))
        log.info("Initial offsets discovered: %s", queue)

        seen_offsets: Set[int] = set()
        html_by_offset: Dict[int, str] = {}
        total_jobs = 0

        # Walk offsets sequentially; new pages can disclose further offsets.
        while queue:
            off = queue.popleft()
            if off in seen_offsets:
                continue
            seen_offsets.add(off)
            page_no = off // STEP_ITEMS + 1

            payload = dict(base_payload); payload["offset"] = str(off)
            html = polite_post(client, action, payload, page_no, off)
            html_by_offset[off] = html

            # Parse & log page count
            jobs_here = parse_teasers(html, off)
            total_jobs += len(jobs_here)
            log.info("← Parsed PAGE %d (offset=%d): %d job(s)", page_no, off, len(jobs_here))

            # Discover new offsets from this response
            newly = [n for n in discover_offsets(html) if n not in seen_offsets and n not in queue]
            if newly:
                log.info("Discovered new offsets from PAGE %d: %s", page_no, newly)
                for n in newly:
                    queue.append(n)

        # Safety step: if the last collected page had STEP_ITEMS, try stepping further by +7
        if html_by_offset:
            last_off = max(html_by_offset)
            def count_cards(h: str) -> int:
                return len(BeautifulSoup(h, "lxml").select("a[href*='/offene-stellen/']"))
            cards_last = count_cards(html_by_offset[last_off])
            while cards_last >= STEP_ITEMS and last_off < 2000:
                next_off = last_off + STEP_ITEMS
                next_page = next_off // STEP_ITEMS + 1
                payload = dict(base_payload); payload["offset"] = str(next_off)
                html = polite_post(client, action, payload, next_page, next_off)
                cards = count_cards(html)
                log.info("Safety step PAGE %d (offset=%d): %d card(s)", next_page, next_off, cards)
                if cards == 0:
                    break
                html_by_offset[next_off] = html
                last_off = next_off
                cards_last = cards

        # Parse all collected HTML (ensures we include safety-step pages)
        all_jobs: List[Job] = []
        for off in sorted(html_by_offset):
            all_jobs.extend(parse_teasers(html_by_offset[off], off))

        # Dedup (title, teaser_url)
        dedup = {(j.title, j.teaser_url): j for j in all_jobs}
        data = [asdict(v) for v in dedup.values()]

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    pages = sorted(html_by_offset)
    log.info("Done. Pages scraped: %d (%s)", len(pages), pages)
    log.info("Total unique jobs: %d", len(data))
    print(f"Wrote {OUTPUT} with {len(data)} jobs across {len(pages)} page(s).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
