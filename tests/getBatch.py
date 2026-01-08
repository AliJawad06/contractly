import csv 
from openai import OpenAI
import json


client = OpenAI()

id = "batch_6908165f1ecc8190821df3dabc13f7ad"


out_id = client.batches.retrieve(id).output_file_id
file_response = client.files.content(out_id).text


new = []

with open('/Users/aj/Downloads/DhikrUp/contractJobs/sample.json', 'r', encoding='utf-8') as i, open("4.1msample.json", 'w') as f:
    reader = json.load(i)

    data = file_response.splitlines()
    

    for i, job in enumerate(reader):
        custom_id = str(i)
        target = next((t for t in data if json.loads(t)['custom_id'] == custom_id), None)
        if not target:
            print("not target")
            continue

        print(target)

        target = json.loads(target)
          
        result_str = target['response']['body']['output'][0]['content'][0]['text']
        new_obj = {**job, "4.1m": result_str}
        new_obj.pop("gemini_2.5_pro", None)
        new.append(new_obj)

    
    json.dump(new, f, indent=2, ensure_ascii=False)    

    

    
    







