import json
import os

file_path = "/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251023_194537.json"
with open(file_path, "r") as r, open ("./jsoncheck.jsonl", 'w') as w:
    data = json.load(r)
    jobs = data["jobs"]
    json.dump(jobs[0], w)
    w.write("\n")
    print(os.path.getsize("./jsoncheck.jsonl")/(1024 * 1024))
    json.dump(jobs[1], w)
    print(os.path.getsize("./jsoncheck.jsonl")/(1024 * 1024))



