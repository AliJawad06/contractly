# commoncrawl_search.py
# Requires: requests
import requests
import json
from urllib.parse import urlparse, parse_qs

# Common Crawl indexes follow the pattern: CC-MAIN-YYYY-WW
# where WW is the week number (not counting from 1, but their internal numbering)
# Recent valid indexes (as of late 2024/early 2025):
# CC-MAIN-2024-51, CC-MAIN-2024-46, CC-MAIN-2024-42, CC-MAIN-2024-38, etc.

# Try the most recent available index
INDEX = "CC-MAIN-2025-43"     # Update this based on https://commoncrawl.org/
PATTERN = "*.jobsapi.ceipal.com*"
URL = f"https://index.commoncrawl.org/{INDEX}-index?url={PATTERN}&output=json"

print(f"Querying Common Crawl index: {INDEX}")
print(f"URL: {URL}\n")

try:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    
    # each line is a JSON result
    urls = []
    for line in resp.text.splitlines():
        if line.strip():  # Skip empty lines
            doc = json.loads(line)
            url = doc.get('url')
            if url:
                urls.append(url)
    
    print(f"Found {len(urls)} URLs from Common Crawl\n")
    
    pairs = []
    for url in urls:
        if 'api_key=' in url:  # Check for Ceipal API URLs
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            
            # Extract api_key and cp_id
            api_key = params.get('api_key', [None])[0]
            cp_id = params.get('cp_id', [None])[0]
            bgcolor = params.get('bgcolor', ['1ba1ff'])[0]
            
            if api_key:
                # Try to extract version from path
                version = 'v1'
                if '/v2/' in url:
                    version = 'v2'
                elif '/v3/' in url:
                    version = 'v3'
                elif '/v4/' in url:
                    version = 'v4'
                
                # Reconstruct clean URL
                clean_url = f"https://jobsapi.ceipal.com/APISource/{version}/index.html?bgcolor={bgcolor}&api_key={api_key}"
                if cp_id:
                    clean_url += f"&cp_id={cp_id}"
                
                pairs.append({
                    'url': clean_url,
                    'company': None,  # Company name not available from URL
                    'api_key': api_key
                })
    
    # Deduplicate by api_key
    unique = list({obj['api_key']: obj for obj in pairs}.values())
    
    print(f"Found {len(unique)} unique Ceipal URLs\n")
    
    # Save as a proper JSON array
    with open('output.json', 'w') as f:
        json.dump(unique, f, indent=2)
    
    print(f"✅ Saved {len(unique)} URLs to output.json")
    
    # Also print them out
    print("\nURLs found:")
    for item in unique[:10]:  # Show first 10
        print(f"  - {item['url']}")
    if len(unique) > 10:
        print(f"  ... and {len(unique) - 10} more")

except requests.exceptions.HTTPError as e:
    print(f"❌ HTTP Error: {e}")
    print("\nThe index might not exist. Try one of these:")
    print("  - CC-MAIN-2024-51")
    print("  - CC-MAIN-2024-46")
    print("  - CC-MAIN-2024-42")
    print("  - CC-MAIN-2024-38")
    print("  - CC-MAIN-2024-33")
    print("\nVisit https://commoncrawl.org/ to find the latest available indexes")
    
except Exception as e:
    print(f"❌ Error: {e}")