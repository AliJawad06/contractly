import json 
from datetime import datetime,timezone,timedelta 
import numbers


def findLast(filepath):
    m_time = 0
    with open(filepath, 'r') as f:
        data = json.load(f)
        jobs = data['jobs']
        for job in jobs:
            time = job["posted"]['date_posted']
            if isinstance(time, numbers.Number) and time > m_time:
                m_time = time
    return m_time

