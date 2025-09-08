import requests
import json

url = "https://ohws.prospective.ch/public/v1/medium/1001134/jobs?lang=de&limit=200&f=25:1140601&offset=0"
response = requests.get(url)
response.raise_for_status()  # Raises an error for bad responses

data = response.json()

with open("usz_jobs.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)