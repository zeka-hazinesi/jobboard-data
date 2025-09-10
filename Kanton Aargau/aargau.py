import requests
import json

url = "https://www.ag.ch/io/jobs-proxy//jobs"

response = requests.get(url)
response.raise_for_status()  # löst eine Exception bei HTTP-Fehlern aus

data = response.json()  # JSON wird direkt eingelesen

with open("ag_jobs.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("✅ JSON gespeichert als ag_jobs.json")
