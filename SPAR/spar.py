import requests
import json

url = "https://www.spar.ch/_api/success_factors_jobs/jobs?itemsPerPage=9999&page=1&companyUids%5B%5D=4&companyUids%5B%5D=9&companyUids%5B%5D=5&companyUids%5B%5D=7&companyUids%5B%5D=2&companyUids%5B%5D=10&companyUids%5B%5D=8&companyUids%5B%5D=3&companyUids%5B%5D=1&companyUids%5B%5D=6&companyUids%5B%5D=0"

response = requests.get(url)
response.raise_for_status()   # falls ein HTTP-Fehler kommt

data = response.json()

# Speichern in eine Datei
with open("spar_jobs.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("✅ JSON gespeichert als spar_jobs.json")
