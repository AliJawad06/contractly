import json 
from datetime import datetime,timezone,timedelta 

m_time = 1761262500000 
with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251023_194537.json', 'r') as f:
    data = json.load(f)
    jobs = data['jobs']
    for job in jobs:
        time = job['posted']['date_posted']
        if time < m_time:
            m_time = time

print(m_time)
ltime = datetime.fromtimestamp((m_time/1000), tz=timezone.utc)
print(ltime)
print(datetime.now())