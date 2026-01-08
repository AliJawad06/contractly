import plotly.graph_objects as go
import pandas as pd
import requests
import time
import json

def geocode_with_photon(location):
    """
    Use Photon geocoder (OpenStreetMap based, no SSL issues)
    Alternative: https://photon.komoot.io
    """
    try:
        url = f"https://nominatim.geocoding.ai/search"
        params = {
            'q': location,
            'format': 'json',
            'limit': 1
        }
        response = requests.get(url, params=params, timeout=10, verify=False)
        data = response.json()
        if data and len(data) > 0:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"Error geocoding {location}: {e}")
    return None

def create_job_map_plotly(jobs_data, output_file='job_map.html'):
    """
    Create an interactive map with job markers using Plotly
    
    Args:
        jobs_data: List of dictionaries with keys: 'title', 'location', 'description'
        output_file: Name of the HTML file to save
    """
    
    # Geocode all locations
    lats = []
    lons = []
    titles = []
    locations = []
    descriptions = []
    
    print(f"Geocoding {len(jobs_data)} locations...")
    
    # Disable SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    for i, job in enumerate(jobs_data):
        try:
            coords = geocode_with_photon(job['location'])
            if coords:
                lats.append(coords[0])
                lons.append(coords[1])
                titles.append(job['title'])
                locations.append(job['location'])
                descriptions.append(job['description'])
                print(f"✓ {i+1}/{len(jobs_data)}: {job['location']}")
            else:
                print(f"✗ {i+1}/{len(jobs_data)}: Could not geocode {job['location']}")
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"✗ {i+1}/{len(jobs_data)}: Error geocoding {job['location']}: {e}")
    
    if not lats:
        print("No jobs could be geocoded!")
        return
    
    # Create DataFrame
    df = pd.DataFrame({
        'lat': lats,
        'lon': lons,
        'title': titles,
        'location': locations,
        'description': descriptions
    })
    
    # Create hover text with HTML formatting
    hover_text = []
    for idx, row in df.iterrows():
        # Create a nicely formatted hover text
        text = f"<b style='font-size:14px'>{row['title']}</b><br>"
        text += f"<span style='color:#7f8c8d'>📍 {row['location']}</span><br><br>"
        desc_preview = row['description'][:250] + '...' if len(row['description']) > 250 else row['description']
        text += f"<span style='font-size:12px'>{desc_preview}</span>"
        hover_text.append(text)
    
    # Create the map
    fig = go.Figure(data=go.Scattergeo(
        lon=df['lon'],
        lat=df['lat'],
        text=hover_text,
        mode='markers',
        marker=dict(
            size=14,
            color='#3498db',
            line=dict(width=2, color='white'),
            symbol='circle',
            opacity=0.9
        ),
        hovertemplate='%{text}<extra></extra>',
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial",
            bordercolor="#3498db"
        )
    ))
    
    # Update layout for better USA view
    fig.update_layout(
        title=dict(
            text=f'<b>Job Locations Map</b><br><sup>{len(df)} positions available</sup>',
            x=0.5,
            xanchor='center',
            font=dict(size=20)
        ),
        geo=dict(
            scope='usa',
            projection_type='albers usa',
            showland=True,
            landcolor='rgb(245, 245, 245)',
            coastlinecolor='rgb(180, 180, 180)',
            showlakes=True,
            lakecolor='rgb(210, 230, 255)',
            showcountries=True,
            countrycolor='rgb(180, 180, 180)',
            bgcolor='rgb(255, 255, 255)'
        ),
        height=700,
        margin=dict(l=0, r=0, t=80, b=0),
        hoverlabel_align='left'
    )
    
    # Save to HTML
    fig.write_html(output_file)
    print(f"\n✓ Map saved to {output_file}")
    print(f"✓ Successfully mapped {len(df)} out of {len(jobs_data)} jobs")
    
    # Also show the figure
    fig.show()
    
    return fig



# Example usage
if __name__ == "__main__":
    # Sample job data
    jobs = []
    with open("/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251103_132542.json", "r") as f:
        jobs = json.load(f)["jobs"]
    
    # Create the map
    create_job_map_plotly(jobs, 'interactive_job_map.html')