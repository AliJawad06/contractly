from bs4 import BeautifulSoup
from pydantic import BaseModel
from typing import List
from google import genai
import json
import json
import asyncio
from google import genai
from geminiLine import getSkills
from google.genai import types
from schema import schema

client = genai.Client(api_key="AIzaSyDS2tks3mC-5WLpUD3MrerKh3Mrjp6ZrVc")


job_name = "batches/f8e8p05cjpa97f8j94o0in141czdll0sqx0t"
batch_job = client.batches.get(name=job_name)

if batch_job.state.name == 'JOB_STATE_SUCCEEDED':

    # If batch job was created with a file
    if batch_job.dest and batch_job.dest.file_name:
        # Results are in a file
        result_file_name = batch_job.dest.file_name
        print(f"Results are in file: {result_file_name}")

        print("Downloading result file content...")
        file_content = client.files.download(file=result_file_name)
        # Process file_content (bytes) as needed
        print(file_content.decode('utf-8'))
        with open('gemini_output.jsonl', 'w') as jsonw: 
            for line in file_content.splitlines():
                    if line:
                        parsed_response = json.loads(line)
                        # Pretty-print the JSON for readability
                        line = json.dumps(parsed_response['response']['candidates'][0]['content'])
                        jsonw.write(line + "\n")
                        print("-" * 20)
                    else:
                        print(f"Job did not succeed. Final state: {batch_job_from_file.state.name}")
