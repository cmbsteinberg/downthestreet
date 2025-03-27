import csv
import json
from typing import List, Dict

def transform_locations_for_mymaps(data: List[Dict]) -> List[Dict]:
    """
    Transform location data into format suitable for Google My Maps import.
    
    Args:
        data: List of dictionaries containing URL, title, and locations data
        
    Returns:
        List of dictionaries formatted for Google My Maps CSV import
    """
    mymaps_data = []
    
    for entry in data:
        url = entry['url']
        title = entry['title']
        
        # Filter for lowest-level locations (non-country)
        lowest_level_locations = [
            loc for loc in entry['locations']
            if loc['location_type'] != 'country'
        ]
        
        # Create a row for each location
        for location in lowest_level_locations:
            mymaps_row = {
                'Name': f"{location['name']} - {title}",
                'Description': f"Source: {url}",
                'Latitude': location['latitude'],
                'Longitude': location['longitude'],
                'Type': location['location_type']
            }
            mymaps_data.append(mymaps_row)
    
    return mymaps_data

def save_to_csv(data: List[Dict], output_file: str):
    """
    Save the transformed data to a CSV file.
    
    Args:
        data: List of dictionaries formatted for Google My Maps
        output_file: Path to save the CSV file
    """
    if not data:
        return
    
    fieldnames = data[0].keys()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Example usage:
if __name__ == "__main__":
    # Sample input data
    sample_data = [
        {
            "url": "https://example.com",
            "title": "Sample Article",
            "locations": [
                {
                    "name": "Mangalore",
                    "official_name": "Mangalore",
                    "country_code": "IN",
                    "longitude": 74.85603,
                    "latitude": 12.91723,
                    "geoname_id": "1263780",
                    "location_type": "city",
                    "population": 417387
                },
                {
                    "name": "🇮🇳",
                    "official_name": "Republic of India",
                    "country_code": "IN",
                    "longitude": 79.0,
                    "latitude": 22.0,
                    "geoname_id": "1269750",
                    "location_type": "country",
                    "population": 1352617328
                }
            ]
        }
    ]
    
    # Transform the data
    mymaps_data = transform_locations_for_mymaps(sample_data)
    
    # Save to CSV
    save_to_csv(mymaps_data, 'locations_for_mymaps.csv')