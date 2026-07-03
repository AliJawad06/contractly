from typing import Dict, Any
from tests.AI_location_parser_batch import Ai_parse_location_batch
from tests.mappers import LocationMapper, TitleMapper
import asyncio
import json
import anthropic
import time
from tests.location_parser import save_mappers, get_mapper_stats
import re
import hashlib
from bs4 import BeautifulSoup
import traceback as tb
# Constants
STATE_ABBREV_ARRAY = [
    {'full_name': 'Alabama', 'abbreviation': 'AL'},
    {'full_name': 'Alaska', 'abbreviation': 'AK'},
    {'full_name': 'Arizona', 'abbreviation': 'AZ'},
    {'full_name': 'Arkansas', 'abbreviation': 'AR'},
    {'full_name': 'California', 'abbreviation': 'CA'},
    {'full_name': 'Colorado', 'abbreviation': 'CO'},
    {'full_name': 'Connecticut', 'abbreviation': 'CT'},
    {'full_name': 'Delaware', 'abbreviation': 'DE'},
    {'full_name': 'Florida', 'abbreviation': 'FL'},
    {'full_name': 'Georgia', 'abbreviation': 'GA'},
    {'full_name': 'Hawaii', 'abbreviation': 'HI'},
    {'full_name': 'Idaho', 'abbreviation': 'ID'},
    {'full_name': 'Illinois', 'abbreviation': 'IL'},
    {'full_name': 'Indiana', 'abbreviation': 'IN'},
    {'full_name': 'Iowa', 'abbreviation': 'IA'},
    {'full_name': 'Kansas', 'abbreviation': 'KS'},
    {'full_name': 'Kentucky', 'abbreviation': 'KY'},
    {'full_name': 'Louisiana', 'abbreviation': 'LA'},
    {'full_name': 'Maine', 'abbreviation': 'ME'},
    {'full_name': 'Maryland', 'abbreviation': 'MD'},
    {'full_name': 'Massachusetts', 'abbreviation': 'MA'},
    {'full_name': 'Michigan', 'abbreviation': 'MI'},
    {'full_name': 'Minnesota', 'abbreviation': 'MN'},
    {'full_name': 'Mississippi', 'abbreviation': 'MS'},
    {'full_name': 'Missouri', 'abbreviation': 'MO'},
    {'full_name': 'Montana', 'abbreviation': 'MT'},
    {'full_name': 'Nebraska', 'abbreviation': 'NE'},
    {'full_name': 'Nevada', 'abbreviation': 'NV'},
    {'full_name': 'New Hampshire', 'abbreviation': 'NH'},
    {'full_name': 'New Jersey', 'abbreviation': 'NJ'},
    {'full_name': 'New Mexico', 'abbreviation': 'NM'},
    {'full_name': 'New York', 'abbreviation': 'NY'},
    {'full_name': 'North Carolina', 'abbreviation': 'NC'},
    {'full_name': 'North Dakota', 'abbreviation': 'ND'},
    {'full_name': 'Ohio', 'abbreviation': 'OH'},
    {'full_name': 'Oklahoma', 'abbreviation': 'OK'},
    {'full_name': 'Oregon', 'abbreviation': 'OR'},
    {'full_name': 'Pennsylvania', 'abbreviation': 'PA'},
    {'full_name': 'Rhode Island', 'abbreviation': 'RI'},
    {'full_name': 'South Carolina', 'abbreviation': 'SC'},
    {'full_name': 'South Dakota', 'abbreviation': 'SD'},
    {'full_name': 'Tennessee', 'abbreviation': 'TN'},
    {'full_name': 'Texas', 'abbreviation': 'TX'},
    {'full_name': 'Utah', 'abbreviation': 'UT'},
    {'full_name': 'Vermont', 'abbreviation': 'VT'},
    {'full_name': 'Virginia', 'abbreviation': 'VA'},
    {'full_name': 'Washington', 'abbreviation': 'WA'},
    {'full_name': 'West Virginia', 'abbreviation': 'WV'},
    {'full_name': 'Wisconsin', 'abbreviation': 'WI'},
    {'full_name': 'Wyoming', 'abbreviation': 'WY'},
    {'full_name': 'District of Columbia', 'abbreviation': 'DC'},
    {'full_name': 'Washington DC', 'abbreviation': 'DC'},
    {'full_name': 'Washington, D.C.', 'abbreviation': 'DC'}
]

# Initialize mappers once at module level
location_mapper = LocationMapper("newmap.json")
title_mapper = TitleMapper("title_mapper.json")


def experience_parser(exp):
            if exp in (None, "N/A", -1):
                exp = -1
            elif type(exp) is int:
                exp = exp
            elif type(exp) is str:
                exp = exp.replace(" year", "")
                exp = int(exp)
            return exp

def data_parser(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse job location and title, using cached mappings when available

    Args:
        job: Job dictionary with 'title', 'location', 'description' keys

    Returns:
        Dictionary with 'category' and/or 'locations' keys
    """
    try:
        res = {}
        req = {}
        description = job.get("description") 
        description =  BeautifulSoup(description, "html.parser").get_text(separator="\n", strip=True)
        job["description"] = description
        # Handle title/category mapping
        experience = job.get("experience") or {}
        exp = experience.get("min_experience") or experience.get("experience")
        exp = experience_parser(exp)
        job["experience"] = exp
        if exp == -1:
            req['yoe'] = job.get('description')
        job_title = job.get('title', "")
        category = title_mapper.get_category(job_title)
        if category:
            res['category'] = category
        else:
            req['title'] = job_title

        # Handle location
        loc = job.get('location', "")
        if not loc:
            req['location'] = job.get('description')
        else:
            cached_location = location_mapper.get_location(loc)
            if cached_location:
                res['locations'] = cached_location
            elif match := re.match(r'^([A-Z]{2}), {1,2}([A-Za-z\s]+)$', loc):
                state = match.group(1)
                city = match.group(2).strip()
                location_data = [{"state": state, "city": city}]
                location_mapper.add_location(loc, location_data)
                res['locations'] = location_data
            else:
                req['location'] = loc
                if req.get('yoe'):
                    req["description"] = job.get("description")

        # Build return object
        row = {}
        if res.get("locations"):
            row["locations"] = res['locations']
            job["locations"] = res['locations']
        if res.get("category"):
            row["category"] = res["category"]
            job["category"] = res["category"]
        if req:
            row["req"] = Ai_parse_location_batch(req)
        if not req:
            row["job"] = job

        return row

    except Exception as e:
        print(f"error {e}")
        tb.print_exc()


def combine_json_files():
    """
    Combine job data from multiple sources, parse locations and titles,
    and save to combined files
    """
    jobsapi_path = "/root/contractly/day/jobsapi_day.json"
    jobdiva_path = "/root/contractly/day/jobdiva_day.json"
    interPath = "/root/contractly/day/day_intermediate.jsonl"

    japi_jobs = []
    diva_jobs = []
    all_jobs = []
    unique_jobs = []
    try:
        with open(jobsapi_path, 'r', encoding='utf-8') as japi_file:
            japi_data = json.load(japi_file)
            japi_jobs = japi_data.get("jobs", [])

        with open(jobdiva_path, "r", encoding='utf-8') as diva_file:
            diva_data = json.load(diva_file)
            diva_jobs = diva_data.get("jobs", [])

        all_jobs = diva_jobs + japi_jobs

        with open(interPath, 'a', encoding='utf-8') as combined:
            print(len(all_jobs))
            unique_jobs = list({item["id"]: item for item in all_jobs}.values())
            print(len(unique_jobs))
            for job in unique_jobs:
                combined.write(json.dumps(job) + "\n")

        output_path = "/root/contractly/day/day.jsonl"
        combined_path = "/root/contractly/combined/all_combined.jsonl"

        with open('/root/contractly/day/day_intermediate.jsonl', 'r') as j, \
             open("/root/contractly/batching_tests.jsonl", 'w') as w, \
             open('/root/contractly/day/day_loc_processing.jsonl', 'w') as loc:
                 
            jobs = unique_jobs
            for i, job in enumerate(jobs, 1):
                try:
                    row = {}
                    url = job.get("apply") or job.get("url") or ""
                    custom_id = hashlib.md5(url.encode()).hexdigest()
                    row["custom_id"] = custom_id
                    job["custom_id"] = custom_id
                    body = data_parser(job)
                    if body.get("locations"):
                        job["location"] = body['locations']
                    if body.get("category"):
                        job["category"] = body.get("category")
                    if body.get("req"):
                        row["params"] = body["req"]
                        w.write(json.dumps(row) + "\n")
                    if body.get("job"):
                        job = body.get("job")
                    loc.write(json.dumps(job) + "\n")
                except Exception as e:
                    print(f"Error processing job {i}: {e}")
                    print(tb.print_exc())
                    print(job)
        
        client = anthropic.Anthropic()
        requests = []
        with open("/root/contractly/batching_tests.jsonl", "r") as f:
            for line in f:
                if line.strip():
                    req = json.loads(line)
                    requests.append(req)
        if requests:
            for attempt in range(3):
                try:
                    batch = client.messages.batches.create(requests=requests)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    print(f"Batch create attempt {attempt + 1} failed: {e}, retrying...")
                    time.sleep(5)
            batch_id = batch.id
            with open("/root/contractly/pending_batch.txt", "w") as f:
                f.write(batch_id)
                print(f"batch_id written to pending_batch.txt {batch_id}")
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    combine_json_files()
