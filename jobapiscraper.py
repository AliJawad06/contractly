# commoncrawl_search.py
# Requires: requests
import requests
import json
from urllib.parse import urlparse, parse_qs

# pick an index (use latest from https://commoncrawl.org/latest-crawl)
INDEX = "CC-MAIN-2025-38"     # check commoncrawl.org/latest-crawl for latest
PATTERN = "https://jobsapi.ceipal.com/APISource/v2/*"   # returns both www1.jobdiva and www2.jobdiva hits
URL = f"https://index.commoncrawl.org/{INDEX}-index?url={PATTERN}&output=json"

resp = requests.get(URL, timeout=30)
resp.raise_for_status()

# each line is a JSON result

urls = []
for line in resp.text.splitlines():
    doc = json.loads(line)
    url = doc.get('url')
    print(url)

