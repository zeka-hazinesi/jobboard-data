import requests
import json

url = "https://www.stadlerrail.com/de/api/prospective-jobs?filter=25:1098730&search="

response = requests.get(url)
response.raise_for_status()  # wirft bei HTTP-Fehlern eine Ausnahme

data = response.json()  # JSON direkt einlesen

# JSON lesbar formatieren und speichern
with open("stadler_jobs.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("✅ JSON gespeichert als stadler_jobs.json")
