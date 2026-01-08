import json
from typing import Dict, Any, Optional

class BaseMapper:
    """Base class for file-backed mapping objects"""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.data = self._load()
    
    def _load(self) -> Dict:
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
    
    def save(self):
        with open(self.filepath, 'w') as f:
            json.dump(self.data, f, indent=2)


class LocationMapper(BaseMapper):
    def get_location(self, location_string: str) -> Optional[Dict]:
        """Get cached location data"""
        return self.data.get(location_string)
    
    def add_location(self, location_string: str, parsed_data: list):
        """Add a new location mapping"""
        self.data[location_string] = parsed_data
    
    def format_locations(self, locations: list) -> list:
        """Format location objects to 'City, State' strings"""
        return [f"{loc['City']}, {loc['State']}" for loc in locations]


class TitleMapper(BaseMapper):
    def get_category(self, title: str) -> Optional[str]:
        """Get category for a title"""
        return self.data.get(title)
    
    def add_title(self, title: str, category: str):
        """Add a new title mapping"""
        self.data[title] = category