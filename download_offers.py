import csv
import os
import re
import time
from datetime import date

import requests

INPUT_CSV = r"c:\Users\enesp\Desktop\unprocessed_data\Jobboard - Data Source.csv"
OUTPUT_DIR = r"c:\Users\enesp\Desktop\unprocessed_data\companies"
REQUEST_TIMEOUT = 15
DELAY_BETWEEN_REQUESTS = 1.0  # seconds


def safe_name(name: str) -> str:
    if not name:
        return "unknown_company"
    name = name.strip()
    # replace characters invalid in Windows filenames
    return re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", name)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    successes = 0
    failures = 0

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            company = row.get("Unternehmen") or row.get("unternehmen") or f"company_{i}"
            url = row.get("offene Stellen Link") or row.get("offene Stellen Link".strip()) or row.get(
                "offene Stellen Link".replace(" ", "")
            )

            company_dir = os.path.join(OUTPUT_DIR, safe_name(company))
            os.makedirs(company_dir, exist_ok=True)

            if not url:
                print(f"[SKIP] {company}: no URL")
                failures += 1
                continue

            url = url.strip()
            if not (url.startswith("http://") or url.startswith("https://")):
                print(f"[SKIP] {company}: invalid URL -> {url}")
                failures += 1
                continue

            try:
                headers = {"User-Agent": "Mozilla/5.0 (compatible; JobboardScraper/1.0)"}
                resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                filename = f"offene_stellen_{date.today().isoformat()}.html"
                out_path = os.path.join(company_dir, filename)
                with open(out_path, "wb") as out:
                    out.write(resp.content)
                print(f"[OK]   {company} -> {out_path}")
                successes += 1
            except requests.RequestException as e:
                print(f"[ERR]  {company}: {e}")
                failures += 1

            time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\nDone. success: {successes}, failed/skipped: {failures}")


if __name__ == "__main__":
    main()