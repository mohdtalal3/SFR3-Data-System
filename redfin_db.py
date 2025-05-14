import requests
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import db_connector

# Constants
SOURCE_NAME = "redfin"  # Source name for database records

# Dictionary of states with their market and region_id
states_config = {
    "Alabama": {"market": "alabama", "region_id": 1},
    "Georgia": {"market": "atlanta", "region_id": 21},
    "Indiana": {"market": "indiana", "region_id": 31},
    "Kansas": {"market": "kansas", "region_id": 35},
    "Kentucky": {"market": "louisville", "region_id": 37},
    "Missouri": {"market": "stlouis", "region_id": 2},
    "North Carolina": {"market": "north-carolina", "region_id": 18},
    "Ohio": {"market": "ohio", "region_id": 22},
    "South Carolina": {"market": "south-carolina", "region_id": 30},
    "Tennessee": {"market": "nashville", "region_id": 34},
    "Arkansas": {"market": "little-rock", "region_id": 7},
    "Wisconsin": {"market": "madison", "region_id": 48},
    "Michigan": {"market": "detroit", "region_id": 47},
}

# Redfin API endpoint
url = "https://www.redfin.com/stingray/api/gis"

# Headers to simulate a browser
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.redfin.com"
}

# Load existing data if any
def load_existing_properties():
    # Since we're not loading from a file anymore, just return an empty list
    return []

def fetch_state_data(state, config):
    print(f"🔍 Scraping: {state}")
    
    # Add random sleep between states (2-4 seconds)
    sleep_time = random.uniform(2, 4)
    time.sleep(sleep_time)
    
    params = {
        "al": 1,
        "include_nearby_homes": "true",
        "max_price": 350000,
        "min_listing_approx_size": 750,
        "min_price": 60000,
        "min_sqft": 750,
        "mpt": 99,
        "num_baths": 1,
        "num_beds": 2,
        "num_homes": 100000,
        "ord": "days-on-redfin-asc",
        "page_number": 1,
        "region_id": config["region_id"],
        "region_type": 4,
        "sf": "1,2,3,7",
        "start": 0,
        "status": 1,
        "uipt": "1,4",
        "v": 8
    }

    state_data = []

    try:
        # Maximum retries
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"➡️ Fetching {state} data (Attempt {attempt+1})")
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200 and response.text.startswith("{}&&"):
                    data = json.loads(response.text[4:])
                    homes = data.get("payload", {}).get("homes", [])
                    
                    print(f"💾 Retrieved {len(homes)} properties for {state}")
                    
                    for home in homes:
                        property_type_raw = home.get("propertyType", -1)
                        formatted_type = "Single_Family" if property_type_raw == 6 else "Multi_Family"

                        parsed = {
                            "property_id": home.get("propertyId", ""),
                            "State": state,
                            "Formatted Property Type": formatted_type,
                            "Occupied/Vacant": "Unknown",  # Not provided
                            "Address": f"{home.get('streetLine', {}).get('value', '')}, {home.get('city', '')}, {home.get('state', '')} {home.get('zip', '')}",
                            "Zip Code": home.get("zip", ""),
                            "Square Footage": home.get("sqFt", {}).get("value", 0),
                            "Rooms (Beds)": home.get("beds", 0),
                            "Bathrooms": home.get("baths", 0),
                            "Year Built": home.get("yearBuilt", {}).get("value", 0),
                            "After Repair Value": home.get("price", {}).get("value", 0),
                            "URL": f"https://www.redfin.com{home.get('url', '')}"
                        }

                        state_data.append(parsed)
                    
                    break  # Success, break the retry loop
                else:
                    print(f"⚠️ Failed at {state}: HTTP {response.status_code}")
                    time.sleep(3 * (attempt + 1))  # Exponential backoff
            except Exception as e:
                print(f"❌ Error at {state}: {str(e)}")
                time.sleep(3 * (attempt + 1))  # Exponential backoff
        
        # If state_data contains properties, save to database
        if state_data:
            print(f"🗄️ Inserting {len(state_data)} properties from {state} into database...")
            inserted, skipped = db_connector.batch_insert_properties(state_data, f"{SOURCE_NAME}")
            print(f"✅ Database insertion complete: {inserted} inserted, {skipped} skipped")
        
    except Exception as e:
        print(f"⚠️ Failed to process {state}: {e}")

    return state_data

def main(states=None):
    print("🚀 Starting Redfin data scraper with database support")
    
    # Ensure database tables exist
    try:
        db_connector.create_tables()
        print("✅ Database tables ready")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return
    
    # Initialize empty list for tracking
    all_homes_data = []
    
    # Use provided states or all states if None
    states_to_process = {}
    if states:
        # Only process states that are valid
        for state in states:
            if state in states_config:
                states_to_process[state] = states_config[state]
            else:
                print(f"⚠️ Invalid state: {state}, skipping")
    else:
        # Process all states
        states_to_process = states_config
    
    print(f"📋 Will process {len(states_to_process)} states: {', '.join(states_to_process.keys())}")
    
    # Process states in a thread pool
    with ThreadPoolExecutor(max_workers=min(2, len(states_to_process))) as executor:
        futures = [executor.submit(fetch_state_data, state, config) for state, config in states_to_process.items()]
        
        for i, future in enumerate(as_completed(futures)):
            state_data = future.result()
            all_homes_data.extend(state_data)
            print(f"✅ Added {len(state_data)} properties to list")
            print(f"Processing {i+1} of {len(states_to_process)} states")

    print(f"✅ Done. Total {len(all_homes_data)} properties saved to database")

if __name__ == "__main__":
    main() 