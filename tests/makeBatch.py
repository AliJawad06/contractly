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

    with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json', 'r') as f:
        data = json.load(f)
        jobs = data['jobs']

    # Randomly sample 1 job (change to 100 later if needed)
    sampled = random.sample(jobs, 100)

    with open('batch.jsonl', 'w', newline='', encoding='utf-8') as f:
        for job in sampled:
                desc = BeautifulSoup(job["description"], "html.parser").get_text(separator=" ", strip=True)
                custom_id = job["id"]
                line = getSkills(desc,custom_id)
                finaline = json.dumps(line)
                f.write(str(finaline) + "\n")

    batch_input_file = await client.files.create(
    file=open("/Users/aj/Downloads/DhikrUp/contractJobs/batch.jsonl", "rb"),
    purpose="batch"
    )

    batch_input_file_id = batch_input_file.id

    await client.batches.create(
        input_file_id=batch_input_file_id,
        endpoint="/v1/responses",
        completion_window="24h"
    )



    

    return "✅ Done writing jsonl"

if __name__ == "__main__":
    result = asyncio.run(runEval())
    print(result)
