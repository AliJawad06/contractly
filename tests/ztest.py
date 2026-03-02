import re
import traceback
from typing import Dict, Any
from AI_location_parser_batch import Ai_parse_location_batch
from mappers import LocationMapper, TitleMapper
import asyncio
import json
from openai import OpenAI
import time
from location_parser import location_parser, save_mappers, get_mapper_stats


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


def location_parser(job: Dict[str, Any]) -> Dict[str, Any]:
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
        
        
        # Handle title/category mapping
        yoe = job.get("experience")
        if yoe == "N/A":
            req['yoe'] = job.get("description")
        job_title = job.get("title", "")
        category = title_mapper.get_category(job_title)
        if category:
            res['category'] = category
        else:
            # Need to get category from AI
            req["title"] = job_title
        
        # Handle location
        loc = job.get("location", "")
        print(loc + "this is location")
        if not loc:
            # If no location, use description as fallback
            req["location"] = job.get("description", "")
        else:
            print(f"Processing location: {loc}")
            
            # Check cache first
            cached_location = location_mapper.get_location(loc)
            if cached_location:
                res['locations'] = location_mapper.format_locations(cached_location)
                return res
            
            # Try regex pattern for "ST, City" format
            pattern = r'^([A-Z]{2}), {1,2}([A-Za-z\s]+)$'
            if match := re.match(pattern, loc):
                state = match.group(1)
                city = match.group(2).strip()
                
                location_data = [{"State": state, "City": city}]
                location_mapper.add_location(loc, location_data)
                res['locations'] = [f"{city}, {state}"]
                
                # If we got location from regex and already have category, return early
                if 'category' in res:
                    return res
            else:
                # Need AI to parse location
                req["location"] = loc
        # If we need AI parsing (either for title or location)
        if req:
            row = Ai_parse_location_batch(req)
            return row

    except Exception as e:
            print(e)

if __name__ == "__main__":
    # You can modify the date cutoff here (default is 30 days)
    with open('/Users/aj/Downloads/DhikrUp/contractJobs/day/day.json', 'r') as j, open("/Users/aj/Downloads/DhikrUp/contractJobs/batching_tests.jsonl", 'w') as w:
        jobs = json.load(j)[0:2]
        for i, job in enumerate(jobs, 1):
            row = {}
            row["custom_id"] = job['url']
            row["method"]="POST"
            row["url"] = "/v1/responses"
            body = location_parser(job)
            print(job["title"])
            row["body"] = body
            w.write(json.dumps(row) + "\n")
    save_mappers()
    client = OpenAI()
    batch_input_file = client.files.create(
    file=open("/Users/aj/Downloads/DhikrUp/contractJobs/batching_tests.jsonl", "rb"),
        purpose="batch"
        )
    batch_input_file_id = batch_input_file.id
    batch = client.batches.create(
                input_file_id=batch_input_file_id,
                endpoint="/v1/responses",
                completion_window="24h",
                metadata={
                    "description": "nightly eval job"
                }
            )
    batch_id = batch.id
    with open("batch_id.json", 'a') as w:
                json.dump({"batch_id": batch_id}, w)
    time.sleep(10)
    batch = client.batches.retrieve(batch_id)
    print(batch.status)
    status = batch.status
    while status != 'completed':
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        time.sleep(10)
        print(status)
    file_response = client.files.content(batch.output_file_id)
    print(file_response.text)
    with open("batch_test_results.jsonl", "w") as bw:
         bw.writelines(file_response.text)

    # After this you would loop through all of the intermediate day.json jobs and add any of new ai fields use .keyiter and then add to that temp array
    # Now this becomes the final form and goes into final form to be updated to supabase 
    

            












    
