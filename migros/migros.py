import requests
import json
import os
import time
import glob

BASE_URL = "https://jobs.migros.ch/api/graphql/query/searchJobs"
PARAMS_TEMPLATE = {
    "__gqlc_language": "de",
    "__gqlh": "v8B6uvsJ4A",   # may change, but works from your example
    "__variables": {
        "jobType": "JOB",
        "page": 1,  # will be updated
        "debug": False,
        "settings": {
            "useFulltext": True,
            "vectorDistanceThreshold": 0.2
        },
        "mode": "RESULTS",
        "perPage": 250
    }
}

OUTPUT_DIR = "job_pages"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_page(page_number: int):
    params = PARAMS_TEMPLATE.copy()
    variables = params["__variables"].copy()
    variables["page"] = page_number
    params["__variables"] = json.dumps(variables)

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()


def save_page(page_number: int, data: dict):
    filepath = os.path.join(OUTPUT_DIR, f"page_{page_number}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def mergeAll():
    hits_all = []
    total = None

    # collect all page_*.json files sorted by page number
    files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "page_*.json")))

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        try:
            searchJobs = data["data"]["searchJobs"]
            if total is None:
                total = searchJobs.get("total")
            hits = searchJobs.get("hits", [])
            hits_all.extend(hits)
        except KeyError:
            print(f"Skipping {file}, unexpected structure.")

    merged = {
        "total": total,
        "hits": hits_all
    }

    with open("migros.json", "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"Merged {len(files)} pages into migros.json with {len(hits_all)} jobs.")


def main():
    page = 1
    while True:
        print(f"Fetching page {page}...")
        data = fetch_page(page)

        save_page(page, data)

        # stop if hits are empty
        try:
            hits = data["data"]["searchJobs"]["hits"]
            if not hits:
                print(f"No more results. Stopping at page {page}.")
                break
        except KeyError:
            print(f"Unexpected response structure on page {page}, stopping.")
            break

        page += 1
        time.sleep(1)

    # merge all pages into one JSON file
    mergeAll()


if __name__ == "__main__":
    main()
