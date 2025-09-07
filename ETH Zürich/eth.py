import requests
from bs4 import BeautifulSoup
import json
import re

def fetch_html(url):
    """
    Fetches the HTML content from the given URL.

    Args:
        url (str): The URL to fetch.

    Returns:
        str: The HTML content if successful, None otherwise.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_ethz_jobs(html_content):
    """
    Extracts job listings from the provided ETH Zurich jobs HTML content.

    Args:
        html_content (str): The full HTML content of the ETH Zurich jobs page.

    Returns:
        list: A list of dictionaries, where each dictionary represents a job
              with extracted details.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    jobs = []
    base_url = "https://jobs.ethz.ch" # Base URL for constructing full job links

    # Find the section containing all job ads
    # The job listings are inside a <ul> with id 'w1' which is inside a <section class='job-ad'>
    job_ads_wrapper = soup.find('ul', id='w1', class_='job-ad__wrapper')

    if not job_ads_wrapper:
        print("Could not find the job ads wrapper. Check HTML structure.")
        return []

    # Iterate through each job item
    # Each job is typically within a div with data-key, which contains an li, which contains an a tag.
    # We target the direct child divs of the ul#w1, as they hold the data-key and the li
    job_divs = job_ads_wrapper.find_all('div', recursive=False)

    for job_div in job_divs:
        job_data = {}

        # Extract job ID from data-key attribute of the parent div
        job_data['id'] = job_div.get('data-key')

        # Find the main link for the job
        job_link_tag = job_div.find('a', class_='job-ad__item__link')
        if not job_link_tag:
            continue

        # Extract URL
        relative_url = job_link_tag.get('href')
        # Construct full URL, handling relative paths
        job_data['url'] = f"{base_url}{relative_url}" if relative_url.startswith('/') else relative_url

        # Extract title
        title_tag = job_link_tag.find('h3', class_='job-ad__item__title')
        job_data['title'] = title_tag.get_text(strip=True) if title_tag else None

        # Extract details (workload, location, term)
        details_tag = job_link_tag.find('div', class_='job-ad__item__details')
        if details_tag:
            details_text = details_tag.get_text(strip=True)
            # Example patterns: "100%, Zurich, fixed-term", "Zürich, Lehrstelle", "10%-20%, Zürich, befristet"
            parts = [p.strip() for p in details_text.split(',')]
            
            job_data['workload'] = None
            job_data['location'] = None
            job_data['term'] = None
            
            # Heuristic to parse details:
            # 1. Look for percentage for workload at the beginning
            # 2. Assume last part is term if it matches specific keywords
            # 3. The remaining part(s) is/are location
            
            # Workload extraction (e.g., "100%", "80%-100%", "10%-20%", "< 40%")
            if parts and re.match(r'(\d+%|\d+%-?\d+%|< ?\d+%)', parts[0]):
                job_data['workload'] = parts.pop(0) # Remove workload from parts
            
            # Term extraction (keywords for fixed-term/permanent/apprenticeship, in German and English)
            term_keywords = ["fixed-term", "permanent", "unbefristet", "befristet", "Lehrstelle"]
            if parts and parts[-1] in term_keywords:
                job_data['term'] = parts.pop(-1) # Remove term from parts
            
            # Location extraction (remaining part(s))
            if parts:
                job_data['location'] = ", ".join(parts).strip()
            
        # Extract company and application deadline
        company_tag = job_link_tag.find('div', class_='job-ad__item__company')
        if company_tag:
            company_text = company_tag.get_text(strip=True)
            # Example: "04.09.2025 | Global Health Engineering group at ETH Zürich"
            # Example: "01.09.2025 | Partnerinserat | ETH Zürich"
            
            parts = [p.strip() for p in company_text.split('|')]
            job_data['application_deadline'] = parts[0] if parts else None

            # Remove deadline and "Partnerinserat" if present, then join remaining for company name
            if len(parts) > 1:
                remaining_parts = parts[1:]
                # Filter out "Partnerinserat" and any empty strings that might result from splitting
                company_name_parts = [p for p in remaining_parts if p not in ["Partnerinserat", ""]]
                job_data['company_department'] = " | ".join(company_name_parts).strip()
            else:
                job_data['company_department'] = None
        
        jobs.append(job_data)
    
    return jobs

# Main execution block
if __name__ == "__main__":
    ETHZ_JOBS_URL = "https://jobs.ethz.ch/"

    print(f"Fetching HTML from: {ETHZ_JOBS_URL}")
    html_content = fetch_html(ETHZ_JOBS_URL)

    if html_content:
        print("HTML successfully retrieved. Extracting jobs...")
        extracted_jobs = extract_ethz_jobs(html_content)

        # Output the jobs in JSON format
        jobs_json = json.dumps(extracted_jobs, indent=4, ensure_ascii=False)
        print(jobs_json)

        print(f"\nSuccessfully extracted {len(extracted_jobs)} jobs.")
        # Optional: Save to a file
        with open('ethz_jobs.json', 'w', encoding='utf-8') as f:
            f.write(jobs_json)
        print("Job data saved to ethz_jobs.json")
    else:
        print("Failed to retrieve HTML content. Cannot extract jobs.")