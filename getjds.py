# commoncrawl_search.py
# Requires: requests
import requests
import json
from urllib.parse import urlparse, parse_qs

# pick an index (use latest from https://commoncrawl.org/latest-crawl)
INDEX = "CC-MAIN-2025-38"     # check commoncrawl.org/latest-crawl for latest
PATTERN = "*.jobdiva.com/*"   # returns both www1.jobdiva and www2.jobdiva hits
URL = f"https://index.commoncrawl.org/{INDEX}-index?url={PATTERN}&output=json"

resp = requests.get(URL, timeout=30)
resp.raise_for_status()

# each line is a JSON result
urls = []
for line in resp.text.splitlines():
    doc = json.loads(line)
    url = doc.get('url')
    if 'www1' in url or 'www2' in url:
        urls.append(url)

pairs = []
for url in urls:
    if 'a=' in url:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        # Get the 'a' parameter
        a_value = params.get('a', [None])[0]
        print(a_value)
        pairs.append({'url': url, 'company': a_value})

# Deduplicate by company
unique = list({obj['company']: obj for obj in pairs}.values())

# ✅ Save as a proper JSON array, not JSONL
with open('output.json', 'w') as f:
    json.dump(unique, f, indent=2)
