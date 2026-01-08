import csv
import json

with open('/Users/aj/Downloads/DhikrUp/contractJobs/five_alt_output.jsonl', 'r', encoding='utf-8') as f, \
     open('/Users/aj/Downloads/DhikrUp/contractJobs/compareAll.csv', 'r', encoding='utf-8') as i, \
     open('tune.csv', 'w', newline='', encoding='utf-8') as c:

    headers = ['input', 'output']
    writer = csv.DictWriter(c, fieldnames=headers)
    writer.writeheader()

    reader = csv.DictReader(i)
    rows = list(reader)




    out_data = [json.loads(job) for job in f]
    #in_data = [json.loads(job) for job in i]
    #for job in in_data:
    for i, job in enumerate(rows):
        custom_id = str(i) #job["custom_id"]
        target = next((t for t in out_data if t["custom_id"] == custom_id), None)
        if not target:
            print("not target")
            continue

        description = job['description']
        output_text = target['response']['body']['output'][1]['content'][0]['text']
        output = json.loads(output_text)

        # Pretty-print JSON for Sheets
        pretty_json = json.dumps(output, indent=2, ensure_ascii=False)

        writer.writerow({
            'input': description.strip(),
            'output': pretty_json
        })