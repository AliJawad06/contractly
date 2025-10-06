import json
from bs4 import BeautifulSoup
import random
import csv

descs = []

template = {
    "required": {
        "softSkills": [],
        "hardSkills": []
    },
    "preferred": {
        "softSkills": [],
        "hardSkills": []
    },
    "yoE": 0,
    "certs": []
}

# Load and clean descriptions
with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json', 'r') as f:
    data = json.load(f)
    jobs = data['jobs']
    for job in jobs:
        desc = job['description']
        soup = BeautifulSoup(desc, "html.parser")
        desc = soup.get_text(separator=" ", strip=True)
        descs.append(desc)

# Randomly sample 100 descriptions
sampled = random.sample(descs, 100)

# ✅ TODO: Write eval to CSV
with open('eval.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["description", "template"])  # header row

    for desc in sampled:
        eval_row = [
            desc,
            json.dumps(template)
        ]
        writer.writerow(eval_row)
