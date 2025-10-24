import json 
import csv
with open('gemini_output.jsonl', 'r') as gemr, open('gem_clean_output.csv', 'w') as gemw:
    data = [json.loads(job) for job in gemr] 
    headers = ["gemini_2.5_pro"]
    writer = csv.DictWriter(gemw, fieldnames=headers)
    writer.writeheader()
    for job in data: 
        result = job["parts"][0]["text"]
        writer.writerow({"gemini_2.5_pro":result})