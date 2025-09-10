# epfl_scientifique_table_to_json.py
# pip install requests beautifulsoup4
import json
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

START_URL = "https://careers.epfl.ch/go/Personnel-Scientifique-%28FR%29/504774/"
OUTFILE = "epfl_personnel_scientifique.json"
PAGE_STEP = 25  # SuccessFactors list pages usually paginate by 25
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def log(msg): print(msg, flush=True)

def find_listing_table(soup: BeautifulSoup):
    """
    Try several common SuccessFactors table selectors, fall back to the first
    <table> that looks like a jobs table (has <thead> or header row).
    """
    candidates = [
        "table#searchresults",
        "table.jobTable",
        "table.job-list",
        "table[role='grid']",
        "table[summary*='emploi'], table[summary*='jobs']",
    ]
    for sel in candidates:
        t = soup.select_one(sel)
        if t: return t

    # Fallback: pick the first table with header cells or a header row
    for t in soup.find_all("table"):
        if t.find("thead") or t.find("th"):
            return t
    return None

def extract_headers(table: BeautifulSoup):
    # Try thead first
    thead = table.find("thead")
    if thead:
        ths = thead.find_all("th")
        headers = [th.get_text(strip=True) for th in ths if th.get_text(strip=True)]
        if headers:
            return headers
    # Fallback: first row as header
    first_tr = table.find("tr")
    if first_tr:
        cells = first_tr.find_all(["th", "td"])
        headers = [c.get_text(strip=True) for c in cells if c.get_text(strip=True)]
        return headers
    return []

def parse_rows(table: BeautifulSoup, base_url: str, headers: list[str]):
    """
    Turn table rows into dicts keyed by headers; also add 'url' and 'id' if found.
    """
    rows = []
    # body rows: skip header row if repeated in tbody
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            # header or spacer row
            continue

        # Map cells to headers (pad/truncate safely)
        values = []
        for td in tds:
            txt = td.get_text(" ", strip=True)
            values.append(txt)
        # Align length
        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        if len(values) > len(headers) and len(headers) > 0:
            values = values[:len(headers)]

        rec = dict(zip(headers, values)) if headers else {"cols": values}

        # Try to find a job link in the row (usually in the title cell)
        a = tr.find("a", href=True)
        if a:
            href = a["href"]
            rec["url"] = href if href.startswith("http") else urljoin(base_url, href)
            # Extract a numeric job id if present in the link
            m = re.search(r"(?:jobId|jobReqId|career_job_req_id)[=/](\d+)", href)
            if not m:
                m = re.search(r"/(\d{4,})/", href)
            if m:
                rec["id"] = m.group(1)

        rows.append(rec)
    return rows

def fetch_page(sess: requests.Session, base_url: str, startrow: int):
    """
    Fetch a single listing page (with pagination via ?startrow=).
    """
    url = base_url
    if not url.endswith("/"):
        url += "/"
    if startrow > 0:
        url = urljoin(url, f"?startrow={startrow}")
    r = sess.get(url, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = find_listing_table(soup)
    if not table:
        log(f"[!] No listing table found at startrow={startrow}")
        return [], []
    headers = extract_headers(table)
    rows = parse_rows(table, base_url, headers)
    return headers, rows

def main():
    sess = requests.Session()
    sess.headers.update(HEADERS)

    all_rows = []
    seen_keys = set()
    headers_master = []

    start = 0
    for _ in range(200):  # hard cap just in case
        log(f"[+] Fetching startrow={start}")
        headers, rows = fetch_page(sess, START_URL, start)
        if rows:
            if not headers_master and headers:
                headers_master = headers
            new = 0
            for r in rows:
                # dedupe by id or title+url or row tuple
                key = r.get("id") or (r.get(headers_master[0], ""), r.get("url", ""))
                if not key:
                    key = tuple(r.values())
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                all_rows.append(r)
                new += 1
            log(f"[✓] startrow={start}: {len(rows)} rows ({new} new)")
            # If fewer than PAGE_STEP rows, likely last page
            if len(rows) < PAGE_STEP:
                break
            start += PAGE_STEP
            time.sleep(0.3)
        else:
            log(f"[✓] No rows at startrow={start} — stopping.")
            break

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)

    log(f"✅ Saved {len(all_rows)} rows to {OUTFILE}")
    if headers_master:
        log(f"ℹ Columns: {headers_master}")

if __name__ == "__main__":
    main()
