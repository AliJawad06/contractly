import csv
import json

# read csv and convert to json
with open('/Users/aj/Downloads/DhikrUp/contractJobs/compareAll.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)


#print(rows[0]['description'])

# write to json
with open('compare_all_output.json', 'w', encoding='utf-8') as f:
    json.dump(rows, f, indent=2, ensure_ascii=False)