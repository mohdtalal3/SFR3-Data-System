import requests
import json
import time
import os
import threading
import queue
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import db_connector

# Constants
BASE_URL = "https://www.realtor.com/frontdoor/graphql"
LIMIT = 200
MAX_OFFSET = 10000
MAX_WORKERS = 5  # Max number of threads
EXISTING_THRESHOLD = 1000  # If we find this many existing properties, stop scraping
SOURCE_NAME = "realtor"  # Source name for database records

# List of states to process
STATES = [
    'Alabama', 'Georgia', 'Indiana', 'Kansas', 'Kentucky', 'Missouri', 
    'North Carolina', 'Ohio', 'South Carolina', 'Tennessee', 'Arkansas', 
    'Wisconsin', 'Michigan'
]
# For testing with a smaller dataset, uncomment:
# STATES = ["Alabama"]

# Property types to process separately
PROPERTY_TYPES = ["single_family", "multi_family"]

# Global dictionary to store all collected properties
all_properties = []

# Set to track property IDs we've already seen
existing_property_ids = set()

# Headers
headers = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://www.realtor.com",
    "Referer": "https://www.realtor.com/realestateandhomes-search/Alabama",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "x-rdc-visitor-id": "366b6d36-b8ac-4c92-ab3e-0703c677a954",
    "rdc-client-name": "RDC_WEB_SRP_FS_PAGE",
    "rdc-client-version": "3.x.x"
}

# GraphQL query string
graphql_query = """query ConsumerSearchQuery($query: HomeSearchCriteria!, $limit: Int, $offset: Int, $search_promotion: SearchPromotionInput, $sort: [SearchAPISort], $sort_type: SearchSortType, $client_data: JSON, $bucket: SearchAPIBucket, $mortgage_params: MortgageParamsInput) {
  home_search: home_search(
    query: $query
    sort: $sort
    limit: $limit
    offset: $offset
    sort_type: $sort_type
    client_data: $client_data
    bucket: $bucket
    search_promotion: $search_promotion
    mortgage_params: $mortgage_params
  ) {
    count
    total
    properties: results {
      property_id
      list_price
      permalink
      description {
        name
        beds
        baths_consolidated
        sqft
        type
        year_built
      }
      location {
        address {
          line
          city
          state_code
          postal_code
        }
      }
    }
  }
}"""

# Load existing properties if the file exists
def load_existing_properties():
    global all_properties, existing_property_ids
    
    # Initialize empty lists/sets since we're not loading from file
    all_properties = []
    existing_property_ids = set()
    
    print(f"✅ Initialized empty property tracking")

# Function to create base payload for a specific state and property type
def create_payload(state, property_type):
    return {
        "operationName": "ConsumerSearchQuery",
        "variables": {
            "query": {
                "primary": True,
                "status": ["for_sale", "ready_to_build"],
                "search_location": {"location": state},
                "baths": {"min": 1},
                "beds": {"min": 2},
                "type": [property_type],
                "sqft": {"min": 800},
                "list_price": {"min": 60000, "max": 350000}
            },
            "client_data": {"device_data": {"device_type": "desktop"}},
            "limit": LIMIT,
            "offset": 0,
            "sort": [
                {"field": "list_date", "direction": "desc"},
                {"field": "photo_count", "direction": "desc"}
            ]
        },
        "query": graphql_query
    }

# Function to map property types to formatted types
def format_property_type(prop_type):
    mapping = {
        "single_family": "Single_Family",
        "multi_family": "Multi_Family"
    }
    return mapping.get(prop_type, "Other")

# Function to transform property data into desired format
def transform_property_data(property_data, full_state_name):
    try:
        # Extract location data
        location = property_data.get("location", {})
        address = location.get("address", {})
        
        # Extract description data
        description = property_data.get("description", {})
        
        # Create full address
        full_address = f"{address.get('line', '')}, {address.get('city', '')}, {address.get('state_code', '')} {address.get('postal_code', '')}"
        
        # Create URL from permalink
        permalink = property_data.get("permalink", "")
        url = f"https://www.realtor.com/realestateandhomes-detail/{permalink}" if permalink else ""
        
        # Format property type
        prop_type = description.get("type", "")
        formatted_type = format_property_type(prop_type)
        
        # Process bathroom value to ensure it's a valid float
        bathroom_value = description.get("baths_consolidated", 0)
        try:
            bathroom_value = float(bathroom_value)
        except (ValueError, TypeError):
            bathroom_value = 0.0
        
        # Create transformed data structure
        transformed = {
            "property_id": property_data.get("property_id", ""),  # Keep property_id for deduplication
            "State": full_state_name,  # Use full state name, not state code
            "Formatted Property Type": formatted_type,
            "Occupied/Vacant": "Unknown",  # Not provided in original data
            "Address": full_address,
            "Zip Code": address.get("postal_code", ""),
            "Square Footage": description.get("sqft", 0),
            "Rooms (Beds)": description.get("beds", 0),
            "Bathrooms": bathroom_value,
            "Year Built": description.get("year_built", 0),
            "After Repair Value": property_data.get("list_price", 0),
            "URL": url
        }
        
        return transformed
    except Exception as e:
        print(f"❌ Error transforming property data: {str(e)}")
        return None

# Function to fetch properties for a specific state, property type, and offset
def fetch_properties(state, property_type, offset):
    property_key = f"{state}_{property_type}_offset_{offset}"
    
    # Add random sleep between requests (2-4 seconds)
    sleep_time = random.uniform(2, 4)
    time.sleep(sleep_time)
    
    # Create payload for this specific request
    payload = create_payload(state, property_type)
    payload["variables"]["offset"] = offset
    
    # Send request with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"➡️ Fetching {state} {property_type} offset {offset} to {offset + LIMIT}... (Attempt {attempt+1})")
            resp = requests.post(BASE_URL, headers=headers, json=payload, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                properties = data["data"]["home_search"]["properties"]
                
                # Transform properties
                transformed_properties = []
                existing_count = 0
                new_count = 0
                
                for prop in properties:
                    property_id = prop.get("property_id", "")
                    
                    # Check if we've already seen this property
                    if property_id in existing_property_ids:
                        existing_count += 1
                        continue
                    
                    # Transform and add the new property
                    transformed = transform_property_data(prop, state)
                    if transformed:
                        transformed_properties.append(transformed)
                        existing_property_ids.add(property_id)  # Mark as seen
                        new_count += 1
                
                print(f"💾 Retrieved {new_count} new and skipped {existing_count} existing {property_type} properties for {state} at offset {offset}")
                
                # Return both the transformed properties and the count of existing ones we found
                return transformed_properties, existing_count, 0
            else:
                print(f"⚠️ Failed at {state} {property_type} offset {offset}: HTTP {resp.status_code}")
                time.sleep(2 * (attempt + 1))  # Exponential backoff
        except Exception as e:
            print(f"❌ Error at {state} {property_type} offset {offset}: {str(e)}")
            time.sleep(2 * (attempt + 1))  # Exponential backoff
    
    print(f"❌ All attempts failed for {state} {property_type} offset {offset}")
    return [], 0, 1

# Function to get total count for a state and property type
def get_total_count(state, property_type):
    payload = create_payload(state, property_type)
    
    print(f"🔍 Getting total count for {state} {property_type}...")
    try:
        response = requests.post(BASE_URL, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            total = data["data"]["home_search"]["total"]
            total_pages = (total + LIMIT - 1) // LIMIT  # Calculate total pages
            
            # Transform first batch of properties
            properties = data["data"]["home_search"]["properties"]
            transformed_properties = []
            existing_count = 0
            
            for prop in properties:
                property_id = prop.get("property_id", "")
                
                # Check if we've already seen this property
                if property_id in existing_property_ids:
                    existing_count += 1
                    continue
                
                # Transform and add the new property
                transformed = transform_property_data(prop, state)
                if transformed:
                    transformed_properties.append(transformed)
                    existing_property_ids.add(property_id)  # Mark as seen
            
            # Add to global properties list
            global all_properties
            all_properties.extend(transformed_properties)
            
            print(f"💾 Retrieved {len(transformed_properties)} new and skipped {existing_count} existing {property_type} properties for {state}")
            return total, total_pages, transformed_properties, existing_count
        else:
            print(f"⚠️ Failed to get count for {state} {property_type}: HTTP {response.status_code}")
            return 0, 0, [], 0
    except Exception as e:
        print(f"❌ Error getting count for {state} {property_type}: {str(e)}")
        return 0, 0, [], 0

# Process a single state and property type with threading for offsets
def process_state_and_property_type(state, property_type):
    print(f"🔄 Processing {state} {property_type}...")
    try:
        # Get the total count for this state and property type
        total_count, total_pages, first_batch_properties, initial_existing_count = get_total_count(state, property_type)
        
        if total_count == 0:
            print(f"⚠️ No {property_type} properties found for {state}")
            return 0, 0
            
        # Calculate the number of pages to request (limit to MAX_OFFSET/LIMIT)
        max_pages = min(total_pages, MAX_OFFSET // LIMIT)
        print(f"📊 Found {total_count} {property_type} properties in {state} ({total_pages} pages, will process up to {max_pages} pages)")
        
        # Save first batch to database
        inserted_count = 0
        skipped_count = 0
        
        if first_batch_properties:
            first_batch_inserted, first_batch_skipped = db_connector.batch_insert_properties(first_batch_properties, SOURCE_NAME)
            inserted_count += first_batch_inserted
            skipped_count += first_batch_skipped
            print(f"✓ Processed first batch: {first_batch_inserted} properties inserted, {first_batch_skipped} skipped")
        
        # Track total existing properties found - include skipped from first batch
        total_existing_count = initial_existing_count + skipped_count
        
        # If we already have too many existing properties, stop early
        if total_existing_count >= EXISTING_THRESHOLD:
            print(f"⚠️ Found {total_existing_count} existing properties for {state} {property_type}, stopping early")
            return inserted_count, skipped_count
        
        # Create offset values for all pages beyond the first
        offsets = [i * LIMIT for i in range(1, max_pages)]  # Skip first page (offset 0)
        
        # Process offsets in smaller batches
        if offsets:
            # Process in smaller batches based on thread count
            batch_size = MAX_WORKERS
            should_break = False  # Flag to track if we need to break out of the main loop
            processed_offsets = 0
            
            for i in range(0, len(offsets), batch_size):
                # Check again if threshold reached before starting a new batch
                if total_existing_count >= EXISTING_THRESHOLD:
                    print(f"⚠️ Found {total_existing_count} existing properties for {state} {property_type}, stopping early")
                    break
                
                # Process the next batch of offsets
                batch_offsets = offsets[i:i+batch_size]
                
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Submit tasks for this batch
                    future_to_offset = {executor.submit(fetch_properties, state, property_type, offset): offset for offset in batch_offsets}
                    
                    # Process results as they complete
                    for future in as_completed(future_to_offset):
                        # Skip if we should already be breaking
                        if should_break:
                            continue
                            
                        # Get the properties and stats from this batch
                        properties, batch_existing, batch_failed = future.result()
                        
                        # Update total existing count
                        total_existing_count += batch_existing
                        
                        # Check if we found too many existing properties
                        if total_existing_count >= EXISTING_THRESHOLD:
                            print(f"⚠️ Found {total_existing_count} existing properties for {state} {property_type}, stopping early")
                            should_break = True
                            # Cancel any remaining futures
                            for f in future_to_offset:
                                if not f.done():
                                    f.cancel()
                            break
                        
                        # Add valid properties to our list
                        if properties:
                            # Add to database in batches
                            batch_inserted, batch_skipped = db_connector.batch_insert_properties(properties, SOURCE_NAME)
                            inserted_count += batch_inserted
                            skipped_count += batch_skipped
                            # Update existing count with newly skipped items
                            total_existing_count += batch_skipped
                        
                        # Update progress
                        processed_offsets += 1
                        print(f"📈 Progress: {processed_offsets}/{len(offsets)} offsets processed, {inserted_count} properties inserted")
                
                # Break out of the main loop if needed
                if should_break or total_existing_count >= EXISTING_THRESHOLD:
                    break
        
        # Print final stats for this state and property type
        print(f"✅ Completed {state} {property_type}: {inserted_count} properties inserted, {skipped_count} skipped")
        return inserted_count, skipped_count
        
    except Exception as e:
        print(f"❌ Error processing {state} {property_type}: {str(e)}")
        return 0, 0

# Main execution
def main(states=None):
    print("🚀 Starting Realtor.com data scraper with database support")
    
    # Ensure database tables exist
    try:
        db_connector.create_tables()
        print("✅ Database tables ready")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return

    # Initialize property tracking
    load_existing_properties()
    
    # Use provided states or all states if None
    states_to_process = states if states else STATES
    print(f"📋 Will process {len(states_to_process)} states: {', '.join(states_to_process)}")

    # Process each state sequentially
    for state in states_to_process:
        if state not in STATES:
            print(f"⚠️ Invalid state: {state}, skipping")
            continue
            
        print(f"🌎 Starting to process state: {state}")
        
        # Process each property type for this state
        for property_type in PROPERTY_TYPES:
            print(f"Processing {state} - {property_type} ({states_to_process.index(state) + 1} of {len(states_to_process)})")
            process_state_and_property_type(state, property_type)
        
        print(f"✅ Completed processing for state: {state}")
        
        # Wait before moving to the next state
        if state != states_to_process[-1]:  # Don't wait after the last state
            # Use random sleep between states (4-7 seconds)
            sleep_time = random.uniform(4, 7)
            print(f"⏱️ Waiting {sleep_time:.2f} seconds before processing next state...")
            time.sleep(sleep_time)

    print(f"✅ Done. All properties saved to database.")

if __name__ == "__main__":
    main() 