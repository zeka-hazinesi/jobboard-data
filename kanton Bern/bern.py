#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, re, time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

START_URL = "https://ohws.prospective.ch/public/v1/careercenter/1001760/?lang=de"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": START_URL,
    "Origin": "https://ohws.prospective.ch",
})

SLEEP = 0.4  # be polite

def bs(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def parse_total_pages(soup: BeautifulSoup) -> Optional[int]:
    m = re.search(r"\b(\d+)\s+von\s+(\d+)\b", soup.get_text(" ", strip=True))
    return int(m.group(2)) if m else None

def extract_jobs_from_page(soup: BeautifulSoup) -> List[Dict]:
    items: List[Dict] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(x in href for x in ("/offene-stellen/", "jobs.apps.", "/job/", "/stellen/")):
            title = " ".join((a.get_text(" ", strip=True) or "").split())
            if not title: 
                continue
            abs_url = href if href.startswith("http") else urljoin(START_URL, href)
            items.append({"title_list": title, "detail_url": abs_url})
    seen, out = set(), []
    for it in items:
        if it["detail_url"] not in seen:
            seen.add(it["detail_url"])
            out.append(it)
    return out

def build_form_payload(form_el: BeautifulSoup) -> Tuple[Dict[str, str], str, str]:
    """
    Return payload, method, action.
    """
    payload: Dict[str, str] = {}
    # inputs
    for inp in form_el.select("input"):
        name = inp.get("name")
        if not name: 
            continue
        t = (inp.get("type") or "text").lower()
        if t in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                payload[name] = inp.get("value") or "on"
        else:
            payload[name] = inp.get("value") or ""
    # selects
    for sel in form_el.select("select"):
        name = sel.get("name")
        if not name: 
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        if opt:
            payload[name] = opt.get("value") or opt.get_text(strip=True)
    # textareas
    for ta in form_el.select("textarea"):
        name = ta.get("name")
        if name:
            payload[name] = ta.get_text() or ""

    method = (form_el.get("method") or "post").lower()
    action = form_el.get("action") or ""
    action = urljoin(START_URL, action) if action else START_URL
    return payload, method, action

def discover_sendPagination(soup: BeautifulSoup) -> Tuple[Optional[int], Optional[str]]:
    """
    Try to find sendPagination(step) and the ID/name of the hidden field it writes to.
    Returns (step, field_id_or_name) where step default is 10.
    """
    step = None
    field = None
    # Next button may hold the step
    next_btn = soup.select_one("#btn-forward[onclick]")
    if next_btn:
        m = re.search(r"sendPagination\((\d+)\)", next_btn.get("onclick", ""))
        if m:
            step = int(m.group(1))
    # Inline JS: look for getElementById('X').value = start;
    for sc in soup.find_all("script"):
        txt = sc.string or sc.get_text() or ""
        if "sendPagination" in txt:
            m1 = re.search(r"getElementById\(['\"]([^'\"]+)['\"]\)\.value\s*=\s*start", txt)
            if m1:
                field = m1.group(1)
    return step, field

def candidate_pagination_fields(form_el: BeautifulSoup) -> List[str]:
    """
    If we couldn't find the exact hidden field ID, propose common candidates by NAME/ID.
    """
    CANDIDATES = ["start", "offset", "from", "page", "pageStart", "firstResult", "resultStart", "startIndex"]
    ids = []
    for inp in form_el.select("input"):
        nm = (inp.get("name") or "").strip()
        id_ = (inp.get("id") or "").strip()
        for c in CANDIDATES:
            if nm == c or id_ == c:
                ids.append(nm or id_)
    # If nothing matched, consider any hidden numeric-looking field
    if not ids:
        for inp in form_el.select('input[type="hidden"]'):
            nm = (inp.get("name") or "").strip()
            if nm and nm not in ids:
                val = (inp.get("value") or "").strip()
                if re.fullmatch(r"\d+", val) or nm.lower() in ("page", "start", "offset"):
                    ids.append(nm)
    return ids

def submit(action: str, method: str, data: Dict[str, str]) -> requests.Response:
    if method.lower() == "get":
        r = SESSION.get(action, params=data, timeout=30)
    else:
        r = SESSION.post(action, data=data, timeout=30)
    r.raise_for_status()
    return r

def scrape_listing() -> List[Dict]:
    # First page
    r0 = SESSION.get(START_URL, timeout=30)
    r0.raise_for_status()
    soup0 = bs(r0.text)

    total_pages = parse_total_pages(soup0)
    print(f"Detected total pages: {total_pages or 'unknown'}")

    # Find *the* form (nearest to the list or containing btn-forward)
    form_el = None
    btn = soup0.select_one("#btn-forward")
    if btn:
        # try to climb up to a form
        p = btn
        for _ in range(5):
            p = p.parent
            if not p: break
            if getattr(p, "name", None) == "form":
                form_el = p
                break
    if not form_el:
        # fallback: first form on page
        form_el = soup0.find("form")
    if not form_el:
        raise RuntimeError("No <form> found on the page—cannot emulate pagination.")

    payload, method, action = build_form_payload(form_el)
    step, exact_field = discover_sendPagination(soup0)
    if not step:
        step = 10  # sensible default for this widget

    # gather jobs on page 1
    all_jobs: Dict[str, Dict] = {}
    for it in extract_jobs_from_page(soup0):
        all_jobs.setdefault(it["detail_url"], it)

    # Figure out which pagination field works by testing candidates
    tests = [exact_field] if exact_field else []
    tests += [f for f in candidate_pagination_fields(form_el) if f and f not in tests]

    if not tests:
        # as a nuclear fallback, also try "start", "offset", "page"
        tests = ["start", "offset", "page"]

    print(f"Trying pagination fields (in order): {tests}")

    worked_field = None
    worked_method = None
    worked_action = None

    # Try several strategies: POST/GET to action and to START_URL
    strategies = []
    # Primary: declared method+action
    strategies.append(("declared", method, action))
    # Also try POST to action
    if method.lower() != "post":
        strategies.append(("force-post", "post", action))
    # Also try POST to current page URL (some forms rely on same URL)
    strategies.append(("post-starturl", "post", START_URL))
    # GET variants (rare but possible)
    strategies.append(("force-get", "get", action))
    strategies.append(("get-starturl", "get", START_URL))

    # Probe which combo moves to page 2 (start = step)
    base_count = len(all_jobs)
    for field in tests:
        for label, meth, act in strategies:
            data = payload.copy()
            data[field] = str(step)
            try:
                resp = submit(act, meth, data)
            except Exception:
                continue
            s = bs(resp.text)
            page_jobs = extract_jobs_from_page(s)
            new = [j for j in page_jobs if j["detail_url"] not in all_jobs]
            if new:
                # Looks promising—remember this combo
                print(f"✔ Pagination works with field='{field}' via {label} ({meth.upper()} {act}) → +{len(new)} jobs")
                worked_field, worked_method, worked_action = field, meth, act
                # merge and continue crawling with this combo
                for it in page_jobs:
                    all_jobs.setdefault(it["detail_url"], it)
                soupN = s
                break
        if worked_field:
            break

    if not worked_field:
        raise RuntimeError("Could not find a working pagination combo. Inspect network requests on the site (XHR).")

    # Continue with the working combo: start = step, 2*step, 3*step...
    start = step * 2  # we already fetched page with start=step
    loops = 1
    while True:
        if total_pages and (start // step) + 1 > total_pages:
            break
        try:
            resp = submit(worked_action, worked_method, {**payload, worked_field: str(start)})
            s = bs(resp.text)
        except Exception:
            break

        page_jobs = extract_jobs_from_page(s)
        new_count = 0
        for it in page_jobs:
            if it["detail_url"] not in all_jobs:
                all_jobs[it["detail_url"]] = it
                new_count += 1

        if new_count == 0:
            # No new items—likely last page
            break

        start += step
        loops += 1
        time.sleep(SLEEP)

    return list(all_jobs.values())

def fetch_detail(url: str) -> Dict:
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    s = bs(r.text)

    def clean(el):
        if not el: return None
        import re as _re
        t = el.get_text("\n", strip=True)
        t = _re.sub(r"[ \t]+", " ", t)
        t = _re.sub(r"\n{3,}", "\n\n", t)
        return t.strip() or None

    title = clean(s.select_one("h1"))
    body_text = s.get_text("\n", strip=True)

    loc = None
    for line in body_text.split("\n")[:15]:
        L = line.strip()
        if 2 <= len(L) <= 40 and re.match(r"^[A-ZÄÖÜ][\wÄÖÜäöüß/ \-]+$", L):
            if L != (title or ""):
                loc = L
                break
    start_text = None
    m = re.search(r"(nach Vereinbarung|per|ab|Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|\b20\d{2}\b).{0,60}", body_text, re.I)
    if m:
        start_text = m.group(0)

    return {
        "title": title,
        "location": loc,
        "start": start_text,
        "detail_url": url,
    }

def main():
    jobs = scrape_listing()
    with open("jobs_overview.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f"Overview: {len(jobs)} jobs")

    # Optional: fetch details for each job
    detailed = []
    for j in jobs:
        try:
            detailed.append({**j, **fetch_detail(j["detail_url"])})
            time.sleep(SLEEP)
        except Exception as e:
            detailed.append({**j, "error": str(e)})
    with open("jobs_detailed.json", "w", encoding="utf-8") as f:
        json.dump(detailed, f, ensure_ascii=False, indent=2)
    print(f"Detailed: {len(detailed)} jobs")

if __name__ == "__main__":
    main()
