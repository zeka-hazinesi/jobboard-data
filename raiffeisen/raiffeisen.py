import requests
import json

URL = "https://ohws.prospective.ch/public/v1/medium/1950/jobs?lang=de&offset=0&limit=300"
OUTPUT_FILE = "raiffeisen_jobs.json"

def download_jobs(url, output_file):
    response = requests.get(url)
    response.raise_for_status()
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    download_jobs(URL, OUTPUT_FILE)