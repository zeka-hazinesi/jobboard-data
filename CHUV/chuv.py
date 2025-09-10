#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json

URL = "https://recrutement.chuv.ch/utf8/ic_job_feeds.feed_engine?p_web_site_id=5352&p_published_to=WWW&p_language=DEFAULT&p_direct=Y&p_format=MOBILE&p_search=&p_summary=Y&p_order=DATE_ON"

def extract_jobs(payload):
    # If the payload is already a list, that's our jobs.
    if isinstance(payload, list):
        return payload
    # Common dict shapes: {"items":[...]}, {"jobs":[...]}, {"results":[...]}, {"data":{"items":[...]}} ...
    if isinstance(payload, dict):
        for key in ("items", "jobs", "results", "rows", "offers", "vacancies"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
            if isinstance(v, dict) and isinstance(v.get("items"), list):
                return v["items"]
        # Fallback: first list value in the dict
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []

def main():
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    jobs = extract_jobs(data)

    with open("chuv_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs if jobs else data, f, ensure_ascii=False, indent=2)

    if jobs:
        print(f"✅ Saved {len(jobs)} jobs to chuv_jobs.json")
    else:
        # If structure is unexpected, we still saved the full payload so you can inspect it.
        # This prints the top-level keys to help adjust quickly.
        keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
        print(f"ℹ️ Saved feed payload to chuv_jobs.json (couldn't auto-find jobs list). Top-level: {keys}")

if __name__ == "__main__":
    main()
