import json
import asyncio
from google import genai
from google.genai import types


client = genai.Client(api_key="AIzaSyDS2tks3mC-5WLpUD3MrerKh3Mrjp6ZrVc")


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
