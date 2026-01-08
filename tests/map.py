import folium
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import ssl
import certifi
from urllib.request import urlopen
import geopy.geocoders
import json
ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx

def geocode_location(location, geolocator):
    """Convert location string to lat/lon coordinates"""
    try:
        loc = geolocator.geocode(location, timeout=10)
        if loc:
            return loc.latitude, loc.longitude
        else:
            print(f"Could not geocode: {location}")
            return None
    except GeocoderTimedOut:
        print(f"Timeout for: {location}")
        return None

def create_job_map(jobs_data, output_file='job_map.html'):
    """
    Create an interactive map with job markers
    
    Args:
        jobs_data: List of dictionaries with keys: 'title', 'location', 'description'
        output_file: Name of the HTML file to save
    """
    
    # Initialize geocoder
    geolocator = Nominatim(user_agent="job_mapper")
    
    # Geocode all locations
    geocoded_jobs = []
    for job in jobs_data:
        coords = geocode_location(job['location'], geolocator)
        if coords:
            geocoded_jobs.append({
                'title': job['title'],
                'location': job['location'],
                'description': job['description'],
                'lat': coords[0],
                'lon': coords[1]
            })
        time.sleep(1)  # Rate limiting for Nominatim
    
    if not geocoded_jobs:
        print("No jobs could be geocoded!")
        return
    
    # Calculate center of map (average of all coordinates)
    avg_lat = sum(job['lat'] for job in geocoded_jobs) / len(geocoded_jobs)
    avg_lon = sum(job['lon'] for job in geocoded_jobs) / len(geocoded_jobs)
    
    # Create map centered on average location
    job_map = folium.Map(
        location=[avg_lat, avg_lon],
        zoom_start=6,
        tiles='OpenStreetMap'
    )
    
    # Add markers for each job
    for job in geocoded_jobs:
        # Create HTML content for popup with scrollable container
        popup_html = f"""
        <div style="width: 300px; max-height: 400px; overflow-y: auto;">
            <h4 style="margin: 0 0 10px 0; color: #2c3e50;">{job['title']}</h4>
            <p style="margin: 5px 0; color: #7f8c8d; font-size: 12px;">
                <strong>📍 Location:</strong> {job['location']}
            </p>
            <hr style="margin: 10px 0;">
            <div style="font-size: 13px; line-height: 1.5; color: #34495e;">
                {job['description']}
            </div>
        </div>
        """
        
        # Create popup with custom styling
        iframe = folium.IFrame(popup_html, width=320, height=420)
        popup = folium.Popup(iframe, max_width=320)
        
        # Add marker with tooltip and popup
        folium.Marker(
            location=[job['lat'], job['lon']],
            popup=popup,
            tooltip=job['title'],  # Shows on hover
            icon=folium.Icon(color='blue', icon='briefcase', prefix='fa')
        ).add_to(job_map)
    
    # Save map
    job_map.save(output_file)
    print(f"Map saved to {output_file}")
    print(f"Successfully mapped {len(geocoded_jobs)} out of {len(jobs_data)} jobs")
    
    return job_map


# Example usage


# Example usage
if __name__ == "__main__":
    # Sample job data
    jobs = []
    with open("/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251103_132542.json", "r") as f:
        jobs = json.load(f)["jobs"]
        

    
    # Create the map
    create_job_map(jobs, 'interactive_job_map.html')