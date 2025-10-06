import time
from datetime import datetime, timedelta, timezone
import json

urls = []
try:
    with open('companies.json', 'r') as f, open('sheetscompanies.json') as s: 
        data = json.load(f)
        sheets = json.load(s)
        for job in data:
            print(job["url"])
            urls.append(job["url"])
        for surls in sheets["jobdiva_urls"]:
            print(surls["url"])
            urls.append(surls["url"])
    print(len(urls))
    print(len(list(set(urls))))    
except Exception:
    print("done")