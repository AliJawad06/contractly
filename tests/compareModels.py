import csv
import json

with open('/Users/aj/Downloads/DhikrUp/contractJobs/batch.jsonl', 'r', encoding='utf-8') as inputFR, \
     open('/Users/aj/Downloads/DhikrUp/nano_batch_output.jsonl', 'r', encoding='utf-8') as nanoFR, \
     open('/Users/aj/Downloads/DhikrUp/contractJobs/batch_output.jsonl', 'r') as fiveFR, \
     open('/Users/aj/Downloads/DhikrUp/mini_batch_output.jsonl','r') as miniFR, \
     open('compare.csv', 'w', newline='', encoding='utf-8') as c:
     


    headers = ['description','mini' ,'nano', 'five']
    writer = csv.DictWriter(c, fieldnames=headers)
    writer.writeheader()

    mini_data = [json.loads(job) for job in miniFR]
    nano_data = [json.loads(job) for job in nanoFR]
    five_data = [json.loads(job) for job in fiveFR]
    in_data = [json.loads(job) for job in inputFR]
    tuplet = []
    for job in in_data:
        custom_id = job["custom_id"]
        mini_target = next((t for t in mini_data if t["custom_id"] == custom_id), None)
        nano_target = next((t for t in nano_data if t["custom_id"] == custom_id), None)
        five_target = next((t for t in five_data if t["custom_id"] == custom_id), None)
        description = job['body']['input'][1]['content']
        if not nano_target and five_target:
                continue
        
        mini_text = json.loads(mini_target['response']['body']['output'][1]['content'][0]['text'])
        nano_text = json.loads(nano_target['response']['body']['output'][1]['content'][0]['text'])
        five_text = json.loads(five_target['response']['body']['output'][1]['content'][0]['text'])
        
        mini_pretty = json.dumps(mini_text, indent=2, ensure_ascii=False)
        nano_pretty = json.dumps(nano_text, indent=2, ensure_ascii=False)
        five_pretty = json.dumps(five_text, indent=2, ensure_ascii=False)
        
        writer.writerow({
             'description': description.strip(),
             'mini': mini_pretty,
             'nano': nano_pretty,
             'five': five_pretty
        })

        
  
        


