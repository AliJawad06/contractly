import json
import asyncio
from google import genai
from tests.gemeni.geminiLine import getSkills
from google.genai import types

def runEval():

    client = genai.Client(api_key="AIzaSyDS2tks3mC-5WLpUD3MrerKh3Mrjp6ZrVc")
    requests = []
    with open('/Users/aj/Downloads/DhikrUp/contractJobs/batch.jsonl', 'r') as inputFR, open('/Users/aj/Downloads/DhikrUp/contractJobs/gemBatch.jsonl', 'w') as gemwriter:
        in_data = [json.loads(job) for job in inputFR]

        for job in in_data:
            description = job['body']['input'][1]['content']
            custom_id = job['custom_id']
            request = getSkills(description, custom_id)
            request = json.loads(request)
            request = json.dumps(request,separators=(', ', ': ') )
            gemwriter.write(request + "\n")

           
    uploaded_file = client.files.upload(
    file='/Users/aj/Downloads/DhikrUp/contractJobs/gemBatch.jsonl',
    config=types.UploadFileConfig(display_name='my-batch-requests', mime_type='jsonl')
)
    print(f"Uploaded file: {uploaded_file.name}")

    file_batch_job = client.batches.create(
    model="gemini-2.5-pro",
    src=uploaded_file.name,
    config={
        'display_name': "file-upload-job-1",
    },
)

    print(f"Created batch job: {file_batch_job.name}")



runEval()