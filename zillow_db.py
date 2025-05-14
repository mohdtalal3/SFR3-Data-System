import requests
import json
import time
import math
import os
import random
from concurrent.futures import ThreadPoolExecutor
import db_connector

# Constants
OUTPUT_DIR = "zillow_properties"
MAX_WORKERS = 10  # Limit concurrent requests to avoid rate limiting
PROPERTY_TYPES = ["single_family", "multi_family"]
SOURCE_NAME = "zillow"  # Source name for database records

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Headers
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.zillow.com/",
    "Origin": "https://www.zillow.com"
    # Add cookies here if required
}

# Region IDs and types for each state
REGION_SELECTION = {
    "Alabama": [{"regionId": 4, "regionType": 2}],
    "Georgia": [{"regionId": 16, "regionType": 2}],
    "Indiana": [{"regionId": 32149, "regionType": 6}],  # Indianapolis
    "Kansas": [{"regionId": 23, "regionType": 2}],
    "Kentucky": [{"regionId": 24, "regionType": 2}],
    "Missouri": [{"regionId": 32, "regionType": 2}],
    "North Carolina": [{"regionId": 36, "regionType": 2}],
    "Ohio": [{"regionId": 44, "regionType": 2}],
    "South Carolina": [{"regionId": 51, "regionType": 2}],
    "Tennessee": [{"regionId": 53, "regionType": 2}],
    "Arkansas": [{"regionId": 6, "regionType": 2}],
    "Wisconsin": [{"regionId": 60, "regionType": 2}],
    "Michigan": [{"regionId": 30, "regionType": 2}]
}

# Map bounds for each state
MAP_BOUNDS = {
    "Alabama": {
        "west": -90.00410075781248,
        "east": -83.35737224218748,
        "south": 28.78443604237234,
        "north": 36.27742418787124
    },
    "Georgia": {
        "north": 36.372274060530245,
        "south": 28.887565985567665,
        "east": -79.8549327421875,
        "west": -86.5016612578125
    },
    "Indiana": {
        "east": -85.72857096777345,
        "north": 40.20610320259475,
        "south": 39.35124490094822,
        "west": -86.55941203222658
    },
    "Kansas": {
        "north": 45.13621770837736,
        "south": 31.223000210969545,
        "east": -91.67334948437501,
        "west": -104.96680651562501
    },
    "Kentucky": {
        "north": 44.52245955178839,
        "south": 30.48004378587517,
        "east": -79.121511484375,
        "west": -92.414968515625
    },
    "Missouri": {
        "north": 44.980152055449224,
        "south": 31.03387779085654,
        "east": -85.79037048437502,
        "west": -99.08382751562502
    },
    "North Carolina": {
        "north": 42.12275628831205,
        "south": 27.59603411133886,
        "east": -73.214265484375,
        "west": -86.507722515625
    },
    "Ohio": {
        "north": 43.696882601690376,
        "south": 36.92048623298724,
        "east": -79.3458882421875,
        "west": -85.9926167578125
    },
    "South Carolina": {
        "north": 37.262567503914184,
        "south": 29.856712933122687,
        "east": -77.60325024218749,
        "west": -84.24997875781249
    },
    "Tennessee": {
        "north": 42.71420640938294,
        "south": 28.303750644995308,
        "east": -79.33187048437499,
        "west": -92.62532751562499
    },
    "Arkansas": {
        "north": 38.34331803373095,
        "south": 31.035923877586246,
        "east": -88.8080142421875,
        "west": -95.4547427578125
    },
    "Wisconsin": {
        "north": 48.01391413959192,
        "south": 41.71609372623012,
        "east": -86.24612624218749,
        "west": -92.89285475781249
    },
    "Michigan": {
        "north": 51.03433818295162,
        "south": 38.47061013504879,
        "east": -79.62382498437502,
        "west": -92.91728201562502
    }
}

# Search terms and state codes
STATE_INFO = {
    "Alabama": {"code": "AL", "searchTerm": "AL", "mapZoom": 7},
    "Georgia": {"code": "GA", "searchTerm": "GA", "mapZoom": 7},
    "Indiana": {"code": "IN", "searchTerm": "Indianapolis, IN", "mapZoom": 7},
    "Kansas": {"code": "KS", "searchTerm": "KS", "mapZoom": 6},
    "Kentucky": {"code": "KY", "searchTerm": "KY", "mapZoom": 6},
    "Missouri": {"code": "MO", "searchTerm": "MO", "mapZoom": 6},
    "North Carolina": {"code": "NC", "searchTerm": "NC", "mapZoom": 6},
    "Ohio": {"code": "OH", "searchTerm": "OH", "mapZoom": 7},
    "South Carolina": {"code": "SC", "searchTerm": "SC", "mapZoom": 7},
    "Tennessee": {"code": "TN", "searchTerm": "TN", "mapZoom": 6},
    "Arkansas": {"code": "AR", "searchTerm": "AR", "mapZoom": 7},
    "Wisconsin": {"code": "WI", "searchTerm": "WI", "mapZoom": 7},
    "Michigan": {"code": "MI", "searchTerm": "MI", "mapZoom": 6}
}

# Base filter state - common across all states
BASE_FILTER_STATE = {
    "sortSelection": {"value": "days"},
    "price": {"min": 60000, "max": 350000},
    "monthlyPayment": {"min": 312, "max": 1817},
    "beds": {"min": 2},
    "baths": {"min": 1},
    "sqft": {"min": 750},
    "built": {"min": 1900},
    "isNewConstruction": {"value": False},
    "isForSaleForeclosure": {"value": False},
    "isComingSoon": {"value": False}
}

# Function to create payload for a state, property type, and page number
def create_payload(state_name, property_type, page_num=1):
    # Create the search query state
    search_query = {
        "pagination": {"currentPage": page_num},
        "isMapVisible": False,
        "mapBounds": MAP_BOUNDS[state_name],
        "regionSelection": REGION_SELECTION[state_name],
        "filterState": dict(BASE_FILTER_STATE),  # Make a copy to avoid modifying the original
        "isListVisible": True,
        "mapZoom": STATE_INFO[state_name]["mapZoom"],
        "usersSearchTerm": STATE_INFO[state_name]["searchTerm"]
    }
    
    # Reset all property type filters first
    search_query["filterState"]["isSingleFamily"] = {"value": False}
    search_query["filterState"]["isMultiFamily"] = {"value": False}
    search_query["filterState"]["isTownhouse"] = {"value": False}
    search_query["filterState"]["isCondo"] = {"value": False}
    search_query["filterState"]["isLotLand"] = {"value": False}
    search_query["filterState"]["isApartment"] = {"value": False}
    search_query["filterState"]["isManufactured"] = {"value": False}
    
    # Set the current property type to True, ensuring a boolean value
    if property_type == "single_family":
        search_query["filterState"]["isSingleFamily"] = {"value": True}
    elif property_type == "multi_family":
        search_query["filterState"]["isMultiFamily"] = {"value": True}
    
    # Create the full payload
    payload = {
        "searchQueryState": search_query,
        "wants": {
            "cat1": ["listResults", "mapResults"],
            "cat2": ["total"]
        },
        "requestId": page_num,
        "isDebugRequest": False
    }
    
    return payload

# Function to format a property
def format_property(home, state_name):
    state_code = STATE_INFO[state_name]["code"]
    home_id = home.get("id", "")
    hdp_info = home.get("hdpData", {}).get("homeInfo", {})
    
    # Get property type and format it
    property_type_raw = hdp_info.get("homeType", "")
    
    # Format property type correctly
    if property_type_raw == "SINGLE_FAMILY":
        formatted_type = "Single_Family"
    elif property_type_raw == "MULTI_FAMILY":
        formatted_type = "Multi_Family"
    else:
        formatted_type = property_type_raw
    
    # Process bathroom value to ensure it's a valid float
    bathroom_value = hdp_info.get("bathrooms", 0)
    try:
        bathroom_value = float(bathroom_value)
    except (ValueError, TypeError):
        bathroom_value = 0.0
    
    parsed = {
        "property_id": home_id,
        "State": state_name,
        "Formatted Property Type": formatted_type,
        "Occupied/Vacant": "Unknown",  # Not available
        "Address": f"{hdp_info.get('streetAddress', '')}, {hdp_info.get('city', '')}, {hdp_info.get('state', '')} {hdp_info.get('zipcode', '')}",
        "Zip Code": hdp_info.get("zipcode", ""),
        "Square Footage": hdp_info.get("livingArea", 0),
        "Rooms (Beds)": hdp_info.get("bedrooms", 0),
        "Bathrooms": bathroom_value,
        "Year Built": hdp_info.get("yearBuilt", 0),
        "After Repair Value": hdp_info.get("price", 0),
        "URL": home.get("detailUrl", "")
    }
    
    # Fix URL to include domain if it's just a path
    if parsed["URL"] and not parsed["URL"].startswith("http"):
        parsed["URL"] = "https://www.zillow.com" + parsed["URL"]
        
    return parsed

# Function to fetch a specific page of results
def fetch_page(state_name, property_type, page_num):
    url = "https://www.zillow.com/async-create-search-page-state"
    payload = create_payload(state_name, property_type, page_num)
    
    # Add random sleep between requests (2-4 seconds)
    sleep_time = random.uniform(2, 4)
    time.sleep(sleep_time)
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"➡️ Fetching {state_name} {property_type} page {page_num} (Attempt {attempt+1})")
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Retrieved page {page_num} for {state_name} {property_type}")
                return data
            else:
                print(f"⚠️ Failed at {state_name} {property_type} page {page_num}: HTTP {response.status_code}")
                time.sleep(3 * (attempt + 1))  # Exponential backoff
        except Exception as e:
            print(f"❌ Error at {state_name} {property_type} page {page_num}: {str(e)}")
            time.sleep(3 * (attempt + 1))  # Exponential backoff
    
    print(f"❌ All attempts failed for {state_name} {property_type} page {page_num}")
    return None

# Function to process a single state and property type
def process_state_property_type(state_name, property_type):
    print(f"🔍 Processing {state_name} - {property_type}")
    
    # Initialize counters
    total_properties = 0
    inserted_count = 0
    skipped_count = 0
    
    try:
        # First, get the first page to determine total results
        first_page_response = fetch_page(state_name, property_type, 1)
        
        # If first page fetch failed, skip this state/type
        if not first_page_response:
            print(f"⚠️ Failed to fetch first page for {state_name} {property_type}. Skipping.")
            return
        
        # Extract results and total count
        if 'cat1' in first_page_response.get('categoryTotals', {}):
            total_results = first_page_response['categoryTotals']['cat1']['totalResultCount']
        else:
            total_results = 0
        
        # Calculate total pages (Zillow limits to 25 pages maximum)
        pages_from_total = math.ceil(total_results / 40)  # 40 results per page
        total_pages = min(pages_from_total, 25)  # Limit to 25 pages
        
        if total_pages == 0:
            print(f"📊 No {property_type} properties found for {state_name}")
            return
        
        print(f"📊 Found {total_results} {property_type} properties in {state_name} (will fetch {total_pages} pages)")
        
        # Extract and process properties from first page
        first_page_properties = []
        if 'cat1' in first_page_response and 'searchResults' in first_page_response['cat1']:
            results = first_page_response['cat1']['searchResults'].get('listResults', [])
            for home in results:
                # Transform each property
                property_data = format_property(home, state_name)
                if property_data:
                    first_page_properties.append(property_data)
        
        # Save first page properties
        if first_page_properties:
            first_page_inserted, first_page_skipped = db_connector.batch_insert_properties(first_page_properties, SOURCE_NAME)
            inserted_count += first_page_inserted
            skipped_count += first_page_skipped
            print(f"✓ Processed page 1: {first_page_inserted} properties inserted, {first_page_skipped} skipped")
        
        # Skip to page 2 since we've already processed page 1
        remaining_pages = list(range(2, total_pages + 1))
        
        # Fetch remaining pages in batches using a thread pool
        if remaining_pages:
            print(f"🔄 Fetching remaining {len(remaining_pages)} pages using {MAX_WORKERS} workers")
            
            # Process pages in batches of 5 to control memory usage
            batch_size = 5
            for i in range(0, len(remaining_pages), batch_size):
                batch_pages = remaining_pages[i:i+batch_size]
                
                # Use thread pool to fetch pages in parallel
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Submit tasks
                    futures = {executor.submit(fetch_page, state_name, property_type, page): page for page in batch_pages}
                    
                    # Process results as they complete
                    for future in futures:
                        page_num = futures[future]
                        try:
                            page_response = future.result()
                            
                            # Skip if page fetch failed
                            if not page_response:
                                print(f"⚠️ Failed to fetch page {page_num} for {state_name} {property_type}")
                                continue
                            
                            # Extract and process properties
                            page_properties = []
                            if 'cat1' in page_response and 'searchResults' in page_response['cat1']:
                                results = page_response['cat1']['searchResults'].get('listResults', [])
                                for home in results:
                                    # Transform each property
                                    property_data = format_property(home, state_name)
                                    if property_data:
                                        page_properties.append(property_data)
                            
                            # Save page properties
                            if page_properties:
                                page_inserted, page_skipped = db_connector.batch_insert_properties(page_properties, SOURCE_NAME)
                                inserted_count += page_inserted
                                skipped_count += page_skipped
                                print(f"✓ Processed page {page_num}: {page_inserted} properties inserted, {page_skipped} skipped")
                            else:
                                print(f"ℹ️ Page {page_num} had no valid properties")
                                
                        except Exception as e:
                            print(f"❌ Error processing page {page_num}: {str(e)}")
                
                # Add a small delay between batches
                if i + batch_size < len(remaining_pages):
                    time.sleep(random.uniform(1, 2))
        
        # Report final stats
        print(f"✅ Completed {state_name} {property_type}: {inserted_count} properties inserted, {skipped_count} skipped")
        
    except Exception as e:
        print(f"❌ Error processing {state_name} {property_type}: {str(e)}")

# Main function
def main(states=None):
    print("🚀 Starting Zillow scraper for multiple states")
    
    # Ensure database tables exist
    try:
        db_connector.create_tables()
        print("✅ Database tables ready")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return
    
    # Initialize global properties list for all states
    all_properties = []
    
    # Use provided states or all states if None
    states_to_process = states if states else list(STATE_INFO.keys())
    print(f"📋 Will process {len(states_to_process)} states: {', '.join(states_to_process)}")
    
    # Process each state
    for state_name in states_to_process:
        if state_name not in STATE_INFO:
            print(f"⚠️ Invalid state: {state_name}, skipping")
            continue
            
        print(f"🌎 Starting to process state: {state_name}")
        
        # First process single_family, then multi_family
        for property_type in PROPERTY_TYPES:
            print(f"Processing {state_name} - {property_type} ({states_to_process.index(state_name) + 1} of {len(states_to_process)})")
            process_state_property_type(state_name, property_type)
            
            # Add random sleep between property types if not the last property type
            if property_type != PROPERTY_TYPES[-1]:
                sleep_time = random.uniform(2, 4)
                print(f"⏱️ Waiting {sleep_time:.2f} seconds before processing next property type...")
                time.sleep(sleep_time)
        
        print(f"✅ Completed processing all property types for {state_name}")
        
        # Wait before moving to the next state
        if state_name != states_to_process[-1]:  # Don't wait after the last state
            # Use random sleep between states (4-7 seconds)
            sleep_time = random.uniform(4, 7)
            print(f"⏱️ Waiting {sleep_time:.2f} seconds before processing next state...")
            time.sleep(sleep_time)
    
    print(f"🎉 All states and property types processed successfully! Total properties: {len(all_properties)}")
    print(f"🗄️ All property data has been stored in the database")

if __name__ == "__main__":
    main() 