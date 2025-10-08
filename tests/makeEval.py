import json
from bs4 import BeautifulSoup
import random
import csv
import asyncio
from getSkills import getSkills

async def runEval():
    # Load jobs
    with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json', 'r') as f:
        data = json.load(f)
        jobs = data['jobs']

    # Randomly sample 1 job (change to 100 later if needed)
    sampled = random.sample(jobs, 1)

    with open('eval.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["description", "response"])  # header row

        for job in sampled:
            try:
                response = await getSkills(job)
                desc = BeautifulSoup(job["description"], "html.parser").get_text(separator=" ", strip=True)
                response_str = json.dumps(response, ensure_ascii=False)
                writer.writerow([desc, response_str])
            except Exception as e:
                print(f"❌ Error processing one job: {e}")

    return "✅ Done writing eval.csv"

if __name__ == "__main__":
    result = asyncio.run(runEval())
    print(result)
