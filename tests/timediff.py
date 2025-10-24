import json
from datetime import datetime,timezone,timedelta 

m_time = 0 
with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251014_131759.json', 'r') as f:
    data = json.load(f)
    jobs = data['jobs']
    for job in jobs:
        time = job['time_scraped']
        
        if time > m_time:
            m_time = time

    ltime = m_time
    for job in jobs:
        time = job['time_scraped']
        if time < ltime:
            ltime = time
    
    diff = m_time - ltime
    diff = diff/1000
    print(timedelta(seconds=diff))
    m_time = m_time/1000
    ltime = ltime/1000
    ogtime = 1760157320000/1000
    m_time = datetime.fromtimestamp(m_time, tz=timezone.utc)
    ltime = datetime.fromtimestamp(ltime, tz=timezone.utc)
    ogtime = datetime.fromtimestamp(ogtime,tz = timezone.utc)


    print(m_time)
    print(ltime)
    print(ogtime)



    

    

