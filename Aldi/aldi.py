import requests
import json

URL = "https://www.jobs.aldi.ch/rest/jobs/search"
OUTPUT_FILE = "aldi_jobs.json"

def download_jobs():
    response = requests.get(URL)
    response.raise_for_status()  # Raises HTTPError for bad responses
    jobs_data = response.json()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(jobs_data, f, ensure_ascii=False, indent=2)
    print(f"Downloaded {len(jobs_data.get('jobs', []))} jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    download_jobs()