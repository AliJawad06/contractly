import re
import traceback
from typing import Dict, Any
from AI_location_parser import Ai_parse_location
from mappers import LocationMapper, TitleMapper


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


async def location_parser(job: Dict[str, Any]) -> Dict[str, Any]:
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
        job_title = job.get("title", "")
        category = title_mapper.get_category(job_title)
        if category:
            res['category'] = category
        else:
            # Need to get category from AI
            req["title"] = job_title
        
        # Handle location
        loc = job.get("location", "")
        
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
            ai_result = await Ai_parse_location(req)
            
            # Handle location results from AI
            if ai_result.get("locations"):
                # Assuming ai_result['locations'] is a list of objects with .city and .state

                locations = [f"{l.get("city", "N/A") or "N/A"}, {l.get("state", "N/A") or "N/A"}" for l in ai_result['locations']]
                location_data = [{"State": l.get("state", "N/A") or "N/A", "City": l.get("city", "N/A") or "N/A"} for l in ai_result["locations"]]
                
                # Cache the result
                if loc:
                    location_mapper.add_location(loc, location_data)
                
                res['locations'] = locations
            
            # Handle category results from AI
            if ai_result.get("category"):
                res['category'] = ai_result['category']
                title_mapper.add_title(job_title, ai_result['category'])
        
        return res
        
    except Exception as e:
        job_url = job.get('url') or job.get('apply', 'N/A')
        print(f"Error parsing location for job {job_url}")
        print(f"Error: {e}")
        print(traceback.format_exc())
        return {}


def save_mappers():
    """
    Save all mapper changes to disk
    Call this after processing all jobs
    """
    print("Saving mapper data...")
    location_mapper.save()
    title_mapper.save()
    print("All mapper data saved")


def get_mapper_stats():
    """Get statistics about cached data"""
    return {
        'locations_cached': len(location_mapper.data),
        'titles_cached': len(title_mapper.data)
    }