import json
from bs4 import BeautifulSoup
import random
import csv
import asyncio
from makeLine import getSkills
from openai import AsyncOpenAI

async def runEval():
    # Load jobs
    client = AsyncOpenAI()
    
    #with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json', 'r') as f:
    #    data = json.load(f)
    #    jobs = data['jobs']

    # Randomly sample 1 job (change to 100 later if needed)
    #sampled = random.sample(jobs, 100)

    with open('/Users/aj/Downloads/DhikrUp/contractJobs/compareAll.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    

    with open('4.1m_alt_batch.jsonl', 'w', newline='', encoding='utf-8') as f:
        for i, job in enumerate(rows):
                desc = job['description'] #BeautifulSoup(job["description"], "html.parser").get_text(separator=" ", strip=True)
                custom_id = i #job["id"]
                line = getSkills(desc,custom_id)
                finaline = json.dumps(line)
                f.write(str(finaline) + "\n")
    batch_input_file = await client.files.create(
    file=open("/Users/aj/Downloads/DhikrUp/contractJobs/4.1m_alt_batch.jsonl", "rb"),
    purpose="batch"
    )

    batch_input_file_id = batch_input_file.id

    batch_info =  await client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/responses",
        completion_window="24h"
    )

    print(batch_info.id)

    return "✅ Done writing jsonl"


if __name__ == "__main__":
    result = asyncio.run(runEval())
