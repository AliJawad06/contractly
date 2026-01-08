import json
from bs4 import BeautifulSoup
import re
with open("/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251109_201037.json", "r") as f:
    jobs = json.load(f)["jobs"]
    for job in jobs:
        desc = BeautifulSoup(job["description"], "html.parser").get_text(separator=" ", strip=True)
        numbers = re.findall(r'\b(\d+)\s+years(?!\s+preferred)\b', desc)
        n = 0
        #if not numbers:
            #if desc.__contains__("Javascript") or desc.__contains__("python") or desc.__contains__("javascript") or desc.__contains__("JS"):
        #    if "Entry Level" in job["title"] or "intern" in job["title"] or "Intern" in job["title"] or "Associate" in job["title"]:
        print("\n" + job["title"] + "\n" + job["url"] + "\n")

    print(len(jobs))
    
            




    



    



