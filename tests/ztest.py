import json

all_clean_path = "all_clean_combined.json"

japi_jobs = []
diva_jobs = []
master_jobs = []
all_clean_jobs = []
        
        
        # Read all input files
with open("/Users/aj/Downloads/DhikrUp/contractJobs/day/day.json", 'r', encoding='utf-8') as clean_file:
    jobs = json.load(clean_file)
    for job in jobs: 
        date_posted = job.get("posted")
        if date_posted == "N/A":
            print(date_posted)
            date_posted = 0
        if  date_posted > 1763164800000 and "CNA" in job["title"]:
            print(job.get("easy_apply_job_login_hc") or job.get("url"), job["location"])