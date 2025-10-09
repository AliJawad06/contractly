import json 

m_time = 0 
with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251008_101452.json', 'r') as f:
    data = json.load(f)
    jobs = data['jobs']
    for job in jobs:
        time = job['posted']['date_posted']
        if time > m_time:
            m_time = time

print(m_time)