import requests
import json
import os
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import hashlib

BASE_URL = ("https://www.post.ch/api/jobs/loadMore/16845b197bac43d9b9e13b79d91ebd50"
            "?jobsCategory=professionals&workload-maximum=1&workload-minimum=0"
            "&startNumber=0&sc_site=post-portal&sc_lang=de")

OUTPUT_DIR = "post_job_pages"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; JobScraper/1.0)",
    "Accept": "application/json",
}

STEP = 5              # Consider 10 to avoid overlapping windows upstream
DELAY_SECONDS = 1

def set_query_param(url: str, key: str, value: str) -> str:
    parts = urlparse(url)
    qs = parse_qs(parts.query, keep_blank_values=True)
    qs[key] = [str(value)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))

def fetch_page(start_number: int) -> dict:
    url = set_query_param(BASE_URL, "startNumber", start_number)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def save_page(start_number: int, data: dict) -> None:
    path = os.path.join(OUTPUT_DIR, f"start_{start_number}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def _extract_items(payload: dict):
    # Common patterns
    for key in ("jobItems", "items", "jobs", "results"):
        v = payload.get(key)
        if isinstance(v, list):
            return v

    # Shallow search for first list value
    for v in payload.values():
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, list):
                    return vv
    return []

def _item_key(item: dict) -> str:
    """
    Produce a stable, unique key for a job item.
    Try common identifier fields first; if none, hash a normalized serialization.
    """
    # Try common fields (adjust if you see a specific schema)
    candidate_keys = [
        "id", "jobId", "jobID", "job_id", "guid", "uuid",
        "jobReference", "reference", "slug", "url", "detailUrl", "applyUrl",
        "jobItemId"
    ]

    # direct
    for k in candidate_keys:
        if k in item and isinstance(item[k], (str, int)):
            return f"{k}:{item[k]}"

    # nested (e.g., item.get("link", {}).get("url"))
    nested_candidates = [
        ("link", "url"),
        ("links", "self"),
        ("meta", "id"),
    ]
    for a, b in nested_candidates:
        v = item.get(a)
        if isinstance(v, dict) and b in v and isinstance(v[b], (str, int)):
            return f"{a}.{b}:{v[b]}"

    # fallback: stable hash of the item
    try:
        normalized = json.dumps(item, sort_keys=True, ensure_ascii=False)
    except TypeError:
        # in case of non-serializable types
        normalized = str(item)
    return "hash:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()

def mergeAll(output_file: str = "swisspost.json") -> None:
    """
    Merge all saved page files into one JSON with duplicate-free items:
    {
      "count": <int>,
      "items": [ ...unique job items... ]
    }
    """
    seen = set()
    items_unique = []

    # Sort by numeric startNumber to keep natural order
    def _startnum_from_name(name: str) -> int:
        try:
            return int(name.split("_")[1].split(".")[0])
        except Exception:
            return 0

    for name in sorted(os.listdir(OUTPUT_DIR), key=_startnum_from_name):
        if not name.startswith("start_") or not name.endswith(".json"):
            continue
        with open(os.path.join(OUTPUT_DIR, name), "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in _extract_items(data):
            key = _item_key(item)
            if key in seen:
                continue
            seen.add(key)
            items_unique.append(item)

    merged = {"count": len(items_unique), "items": items_unique}
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"Merged {len(items_unique)} unique items into {output_file}")

def main():
    start = 0
    while True:
        print(f"Fetching startNumber={start} ...")
        data = fetch_page(start)
        save_page(start, data)

        has_more = data.get("hasMoreJobItems")
        if has_more is False:
            print(f"hasMoreJobItems is False at startNumber={start}. Stopping.")
            break
        if has_more is None:
            print(f"'hasMoreJobItems' not found at startNumber={start}. Stopping defensively.")
            break

        start += STEP
        time.sleep(DELAY_SECONDS)

    # Merge everything into one file (deduped)
    mergeAll("swisspost.json")

if __name__ == "__main__":
    main()
