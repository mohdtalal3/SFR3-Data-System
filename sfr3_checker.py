import mysql.connector
import os
import sys
import time
import random
import requests
from dotenv import load_dotenv
import db_connector
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("verification.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sfr3_checker")

# Counter for API requests
api_request_counter = 0

# Load environment variables from .env file
load_dotenv()

# Default configuration
DEFAULT_BATCH_SIZE = 1000  # Default batch size
RECOMMENDED_LIMIT = 5000  # Recommended maximum properties to process at once
MAX_RETRIES = 3  # Maximum number of retries for db operations
DB_BATCH_SIZE = 100  # Size of batches for database updates

def get_connection():
    """Get a database connection."""
    try:
        connection = db_connector.get_db_connection()
    except Exception as e:
        logger.error(f"Error getting connection: {e}")
        # Sleep and retry
        time.sleep(0.5)
        connection = db_connector.get_db_connection()
    
    return connection

def close_connection(connection):
    """Close the database connection."""
    if connection and connection.is_connected():
        connection.close()

def get_properties_to_verify(batch_size=DEFAULT_BATCH_SIZE, source=None, include_failed=True, retry_api_only=False):
    """Get a batch of properties from the database."""
    connection = get_connection()
    
    for retry in range(MAX_RETRIES):
        try:
            cursor = connection.cursor(dictionary=True)
            
            # Prepare query based on filters
            if retry_api_only:
                # Only get properties with API_ERROR
                if source:
                    query = """
                    SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                    FROM properties
                    WHERE failure_reason = 'API_ERROR' AND source LIKE %s
                    ORDER BY date_added ASC
                    LIMIT %s
                    """
                    cursor.execute(query, (f"%{source}%", batch_size))
                    logger.info(f"Querying API errors only with source={source}")
                else:
                    query = """
                    SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                    FROM properties
                    WHERE failure_reason = 'API_ERROR'
                    ORDER BY date_added ASC
                    LIMIT %s
                    """
                    cursor.execute(query, (batch_size,))
                    logger.info(f"Querying API errors only, no source filter")
            elif source and include_failed:
                query = """
                SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                FROM properties
                WHERE (is_verified = FALSE OR failure_reason = 'API_ERROR') AND source LIKE %s
                ORDER BY date_added ASC
                LIMIT %s
                """
                cursor.execute(query, (f"%{source}%", batch_size))
                logger.info(f"Querying with source={source} and include_failed=True")
            elif source and not include_failed:
                query = """
                SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                FROM properties
                WHERE is_verified = FALSE AND (failure_reason IS NULL OR failure_reason = 'API_ERROR') AND source LIKE %s
                ORDER BY date_added ASC
                LIMIT %s
                """
                cursor.execute(query, (f"%{source}%", batch_size))
                logger.info(f"Querying with source={source} and include_failed=False")
            elif include_failed:
                query = """
                SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                FROM properties
                WHERE is_verified = FALSE OR failure_reason = 'API_ERROR'
                ORDER BY date_added ASC
                LIMIT %s
                """
                cursor.execute(query, (batch_size,))
                logger.info(f"Querying with include_failed=True, no source filter")
            else:
                query = """
                SELECT id, property_id, state, property_type, address, url, source, is_verified, failure_reason
                FROM properties
                WHERE is_verified = FALSE AND (failure_reason IS NULL OR failure_reason = 'API_ERROR')
                ORDER BY date_added ASC
                LIMIT %s
                """
                cursor.execute(query, (batch_size,))
                logger.info(f"Querying with include_failed=False, no source filter")
            
            properties = cursor.fetchall()
            logger.info(f"Retrieved {len(properties)} properties for checking")
            
            # Debug: If no properties found, let's check how many properties exist at all
            if len(properties) == 0:
                cursor.execute("SELECT COUNT(*) as total FROM properties")
                total = cursor.fetchone()['total']
                logger.info(f"Total properties in database: {total}")
                
                cursor.execute("SELECT COUNT(*) as unverified FROM properties WHERE is_verified = FALSE")
                unverified = cursor.fetchone()['unverified']
                logger.info(f"Total unverified properties: {unverified}")
                
                if source:
                    cursor.execute("SELECT COUNT(*) as matching FROM properties WHERE source LIKE %s", (f"%{source}%",))
                    matching = cursor.fetchone()['matching']
                    logger.info(f"Properties matching source '{source}': {matching}")
                
                if retry_api_only:
                    cursor.execute("SELECT COUNT(*) as api_errors FROM properties WHERE failure_reason = 'API_ERROR'")
                    api_errors = cursor.fetchone()['api_errors']
                    logger.info(f"Properties with API_ERROR: {api_errors}")
            
            cursor.close()
            return properties
        
        except mysql.connector.Error as err:
            logger.error(f"Error retrieving properties: {err}")
            if retry < MAX_RETRIES - 1:
                logger.info(f"Retrying... ({retry + 1}/{MAX_RETRIES})")
                time.sleep(1)  # Wait before retrying
                # Refresh connection
                try:
                    connection.close()
                except:
                    pass
                connection = get_connection()
            else:
                logger.error("Max retries reached. Giving up.")
                return []
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
    
    return []

def get_property_details(property_id, connection):
    """Get detailed information for a specific property."""
    for retry in range(MAX_RETRIES):
        try:
            cursor = connection.cursor(dictionary=True)
            query = """
            SELECT *
            FROM properties
            WHERE property_id = %s
            """
            cursor.execute(query, (property_id,))
            property_data = cursor.fetchone()
            cursor.close()
            return property_data
        
        except mysql.connector.Error as err:
            logger.error(f"Error retrieving property details: {err}")
            if retry < MAX_RETRIES - 1:
                logger.info(f"Retrying... ({retry + 1}/{MAX_RETRIES})")
                time.sleep(1)  # Wait before retrying
                # Refresh connection
                try:
                    connection.close()
                except:
                    pass
                connection = get_connection()
            else:
                logger.error("Max retries reached. Giving up.")
                return None
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
    
    return None

def verify_property(property_data):
    """
    Verify a property based on:
    1. Square footage must be at least 800
    2. SFR3 API must not return interested=false
    3. API must return status code 200
    
    Returns:
        tuple: (is_verified, failure_reason)
    """
    global api_request_counter
    
    # First check: square footage must be at least 800
    if property_data.get('square_footage', 0) < 800:
        logger.info(f"Property {property_data['property_id']} failed verification: square footage ({property_data.get('square_footage', 0)}) < 800")
        return False, "SQUARE_FOOTAGE"
    
    # Second check: check with SFR3 API
    try:
        address = property_data.get('address', '')
        if not address:
            logger.warning(f"Property {property_data['property_id']} has no address, cannot check with API")
            return False, "NO_ADDRESS"

        # API Endpoint
        url = "http://api.sfr3.com/sfr3/offmarket/check-address"

        # Send request to SFR3 API
        logger.info(f"Checking property {property_data['property_id']} address with SFR3 API: {address}")
        
        try:
            # Increment API request counter and check if pause needed
            api_request_counter += 1
            
            # Pause after every 100 API requests
            if api_request_counter % 100 == 0:
                logger.info(f"Pausing for 5 seconds after {api_request_counter} API requests...")
                time.sleep(5)  # 5 second pause
            
            response = requests.get(url, params={"address": address}, timeout=10)
            
            # Handle rate limiting responses
            if response.status_code == 400:
                try:
                    response_data = response.json()
                    error_message = response_data.get("message", "")
                    if "Too many requests" in error_message:
                        logger.warning(f"SFR3 API rate limit exceeded for property {property_data['property_id']}: {error_message}")
                        return False, "API_ERROR"
                except Exception:
                    # If we can't parse the response, still treat as API error
                    logger.warning(f"SFR3 API returned status 400 for property {property_data['property_id']}")
                    return False, "API_ERROR"
            
            if response.status_code == 200:
                data = response.json()
                interested = data.get("interested")
                reason = data.get("reason", "No reason provided")
                
                if interested is False:
                    logger.info(f"Property {property_data['property_id']} failed SFR3 API check: {reason}")
                    return False, "NOT_INTERESTED"
                else:
                    logger.info(f"Property {property_data['property_id']} passed SFR3 API check with response: {interested}, reason: {reason}")
                    return True, None
            else:
                logger.warning(f"SFR3 API returned non-200 status code {response.status_code} for property {property_data['property_id']}")
                return False, "API_ERROR"
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to SFR3 API for property {property_data['property_id']}: {str(e)}")
            return False, "API_ERROR"
            
    except Exception as e:
        logger.error(f"Error checking property {property_data['property_id']} with SFR3 API: {str(e)}")
        return False, "API_ERROR"
    
    # If we get here, the property passed all checks
    return True, None

def batch_update_verification_status(update_batch, connection):
    """Update verification status for multiple properties in a single batch."""
    if not update_batch:
        return 0, 0
        
    # Separate the data for verified and non-verified properties
    verified_ids = [item['property_id'] for item in update_batch if item['is_verified']]
    
    # Group non-verified properties by failure reason
    failure_groups = {}
    for item in update_batch:
        if not item['is_verified']:
            reason = item['failure_reason']
            if reason not in failure_groups:
                failure_groups[reason] = []
            failure_groups[reason].append(item['property_id'])
    
    updated_count = 0
    failed_count = 0
    
    try:
        cursor = connection.cursor()
        
        # Update verified properties in one query
        if verified_ids:
            verified_placeholders = ', '.join(['%s'] * len(verified_ids))
            verified_query = f"UPDATE properties SET is_verified = TRUE, failure_reason = NULL WHERE property_id IN ({verified_placeholders})"
            cursor.execute(verified_query, verified_ids)
            updated_count += cursor.rowcount
            
        # Update each group of non-verified properties with the same failure reason
        for reason, ids in failure_groups.items():
            if ids:
                failure_placeholders = ', '.join(['%s'] * len(ids))
                if reason is None:
                    failure_query = f"UPDATE properties SET is_verified = FALSE, failure_reason = NULL WHERE property_id IN ({failure_placeholders})"
                    cursor.execute(failure_query, ids)
                else:
                    failure_query = f"UPDATE properties SET is_verified = FALSE, failure_reason = %s WHERE property_id IN ({failure_placeholders})"
                    cursor.execute(failure_query, [reason] + ids)
                updated_count += cursor.rowcount
        
        # Commit the transaction
        connection.commit()
        logger.info(f"Batch updated {updated_count} properties successfully")
        
        # Count any missing property IDs
        total_in_batch = len(update_batch)
        if updated_count < total_in_batch:
            failed_count = total_in_batch - updated_count
            logger.warning(f"Failed to update {failed_count} properties (not found in database)")
            
    except mysql.connector.Error as err:
        logger.error(f"Error in batch update: {err}")
        connection.rollback()
        failed_count = len(update_batch)
    finally:
        cursor.close()
    
    return updated_count, failed_count

def process_single_property(prop, connection, update_batch):
    """Process a single property for verification."""
    property_id = prop['property_id']
    result = {
        'verified': 0,
        'failed': 0,
        'api_error': 0,
        'square_footage': 0,
        'not_interested': 0,
        'no_address': 0
    }
    
    # Skip already verified properties
    if prop.get('is_verified'):
        logger.info(f"Skipping already verified property {property_id}")
        return result
        
    # For properties with API_ERROR, we want to retry
    # For other failure reasons, skip them
    if prop.get('failure_reason') and prop.get('failure_reason') != 'API_ERROR':
        logger.info(f"Skipping property {property_id} with permanent failure reason: {prop.get('failure_reason')}")
        return result
    
    # Get detailed property information
    property_details = get_property_details(property_id, connection)
    
    if not property_details:
        logger.warning(f"Could not retrieve details for property {property_id}")
        return result
    
    # Add a small random delay to avoid overloading the system and API rate limits
    time.sleep(random.uniform(0.1, 0.3))
    
    # Verify the property
    is_verified, failure_reason = verify_property(property_details)
    
    # Add to update batch instead of updating immediately
    update_batch.append({
        'property_id': property_id,
        'is_verified': is_verified,
        'failure_reason': failure_reason
    })
    
    # Update result counts
    if is_verified:
        result['verified'] = 1
    else:
        result['failed'] = 1
        
        # Count by failure reason
        if failure_reason == 'API_ERROR':
            result['api_error'] = 1
        elif failure_reason == 'SQUARE_FOOTAGE':
            result['square_footage'] = 1
        elif failure_reason == 'NOT_INTERESTED':
            result['not_interested'] = 1
        elif failure_reason == 'NO_ADDRESS':
            result['no_address'] = 1
    
    return result

def process_verification_batch(properties):
    """Process a batch of properties for verification sequentially."""
    connection = get_connection()
    
    total_results = {
        'verified': 0,
        'failed': 0,
        'api_error': 0,
        'square_footage': 0,
        'not_interested': 0,
        'no_address': 0
    }
    
    # Batch for database updates
    update_batch = []
    updates_processed = 0
    DB_UPDATE_BATCH_SIZE = 100  # Update database after every 100 properties
    
    try:
        for prop in properties:
            try:
                result = process_single_property(prop, connection, update_batch)
                # Update total counts
                for key in total_results:
                    total_results[key] += result.get(key, 0)
                
                # Check if we need to process a batch of updates
                if len(update_batch) >= DB_UPDATE_BATCH_SIZE:
                    updated, failed = batch_update_verification_status(update_batch, connection)
                    updates_processed += updated
                    update_batch = []  # Clear the batch
                    logger.info(f"Processed batch update of {updated} properties")
            except Exception as exc:
                property_id = prop.get('property_id', 'unknown')
                logger.error(f"Property {property_id} generated an exception: {exc}")
        
        # Process any remaining updates
        if update_batch:
            updated, failed = batch_update_verification_status(update_batch, connection)
            updates_processed += updated
            logger.info(f"Processed final batch update of {updated} properties")
    
    finally:
        close_connection(connection)
    
    return total_results

def add_failure_reason_column():
    """Add a failure_reason column to the properties table if it doesn't exist."""
    connection = get_connection()
    
    for retry in range(MAX_RETRIES):
        try:
            cursor = connection.cursor()
            
            # Check if failure_reason column exists
            cursor.execute("SHOW COLUMNS FROM properties LIKE 'failure_reason'")
            column_exists = cursor.fetchone() is not None
            
            if not column_exists:
                # Add the column
                cursor.execute("ALTER TABLE properties ADD COLUMN failure_reason VARCHAR(50) NULL")
                connection.commit()
                logger.info("Added failure_reason column to properties table")
            else:
                logger.info("failure_reason column already exists in properties table")
            
            cursor.close()
            break
                
        except mysql.connector.Error as err:
            logger.error(f"Error checking/adding failure_reason column: {err}")
            if retry < MAX_RETRIES - 1:
                logger.info(f"Retrying... ({retry + 1}/{MAX_RETRIES})")
                time.sleep(1)  # Wait before retrying
                # Refresh connection
                try:
                    connection.close()
                except:
                    pass
                connection = get_connection()
            else:
                logger.error("Max retries reached. Giving up.")
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
    
    close_connection(connection)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="SFR3 Property Verification Tool")
    
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Number of properties to process in each batch (default: {DEFAULT_BATCH_SIZE})")
    
    # Keep the threads parameter for backward compatibility but ignore it
    parser.add_argument("--threads", type=int, 
                        help="Ignored - for backward compatibility only. Processing is now sequential.")
    
    parser.add_argument("--source", type=str, 
                        help="Filter properties by source (e.g., 'realtor', 'zillow')")
    
    parser.add_argument("--limit", type=int,
                        help="Maximum number of properties to verify")
    
    parser.add_argument("--retry-api-only", action="store_true",
                        help="Only process properties that failed with API_ERROR")
    
    parser.add_argument("--skip-failed", action="store_true",
                        help="Skip properties that have failed for reasons other than API_ERROR")
    
    parser.add_argument("--db-batch-size", type=int, default=DB_BATCH_SIZE,
                        help=f"Size of database update batches (default: {DB_BATCH_SIZE})")
    
    # Handle legacy command-line format for backward compatibility
    args, unknown = parser.parse_known_args()
    
    # If we have old-style arguments, process them
    if unknown and len(sys.argv) > 1:
        # First argument as source or flag
        if sys.argv[1].lower() == '--retry-api-only' and not args.retry_api_only:
            args.retry_api_only = True
        elif sys.argv[1].lower() == '--skip-failed' and not args.skip_failed:
            args.skip_failed = True
        elif sys.argv[1].lower().startswith('--threads'):
            # Ignore --threads argument in old-style format
            pass
        elif sys.argv[1].isdigit() and not args.limit:
            args.limit = int(sys.argv[1])
        elif not args.source and not sys.argv[1].startswith('--'):
            args.source = sys.argv[1]
            
        # Second argument as limit
        if len(sys.argv) > 2 and sys.argv[2].isdigit() and not args.limit:
            args.limit = int(sys.argv[2])
    
    return args

def main():
    print("🔍 Starting SFR3 Property Verification Process")
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Extract arguments
    batch_size = args.batch_size
    source = args.source
    total_properties = args.limit
    include_failed = not args.skip_failed
    retry_api_only = args.retry_api_only
    
    # Update the global DB_BATCH_SIZE if specified
    global DB_BATCH_SIZE
    if args.db_batch_size and args.db_batch_size != DB_BATCH_SIZE:
        DB_BATCH_SIZE = args.db_batch_size
    
    # Reset API counter at start
    global api_request_counter
    api_request_counter = 0
    
    # Display configuration
    print(f"⚙️ Configuration:")
    print(f"   Batch Size: {batch_size} properties")
    print(f"   Sequential processing (no threading)")
    print(f"   API Rate Limits: Pause for 5s every 100 requests")
    if source:
        print(f"   Source Filter: {source}")
    if total_properties:
        print(f"   Property Limit: {total_properties}")
    if retry_api_only:
        print("   Processing only properties with API errors")
    elif not include_failed:
        print("   Skipping properties with permanent failure reasons")
    
    # Debug parameter info
    logger.info(f"Running with parameters: batch_size={batch_size}, source={source}, limit={total_properties}, include_failed={include_failed}, retry_api_only={retry_api_only}")
    
    # Show warning for large property limits
    if total_properties and total_properties > RECOMMENDED_LIMIT:
        print(f"⚠️ Warning: Processing more than {RECOMMENDED_LIMIT} properties at once may take a long time.")
        print(f"   Consider using a smaller limit for better performance and to avoid API rate limits.")
    
    try:
        # Add failure_reason column if it doesn't exist
        add_failure_reason_column()
        
        properties_verified = 0
        total_counts = {
            'verified': 0,
            'failed': 0,
            'api_error': 0,
            'square_footage': 0,
            'not_interested': 0,
            'no_address': 0
        }
        
        # Process properties in batches
        while True:
            # If we have a total limit, adjust the batch size for the last batch
            if total_properties is not None:
                remaining = total_properties - properties_verified
                if remaining <= 0:
                    print(f"✅ Reached the limit of {total_properties} properties. Process complete.")
                    break
                    
                # Adjust batch size for last batch if needed
                current_batch_size = min(batch_size, remaining)
            else:
                current_batch_size = batch_size
            
            properties = get_properties_to_verify(current_batch_size, source, include_failed, retry_api_only)
            
            if not properties:
                print("✅ No more properties to verify. Process complete.")
                break
            
            print(f"🔄 Processing batch of {len(properties)} properties sequentially...")
            batch_counts = process_verification_batch(properties)
            properties_verified += len(properties)
            
            # Update total counts
            for key in total_counts:
                total_counts[key] += batch_counts.get(key, 0)
            
            print(f"📊 Batch results: {batch_counts['verified']} verified, {batch_counts['failed']} failed verification")
            print(f"📊 Total progress: {properties_verified} / {total_properties if total_properties else 'all'} properties processed")
            print(f"   Failure breakdown: {batch_counts['square_footage']} square footage, "
                 f"{batch_counts['not_interested']} not interested, "
                 f"{batch_counts['no_address']} no address, "
                 f"{batch_counts['api_error']} API errors")
            
            # Add a smaller delay between batches
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n⚠️ Verification process interrupted by user.")
    except Exception as e:
        logger.error(f"Error during verification process: {e}")
        print(f"❌ An error occurred: {e}")
    
    print(f"📈 Summary: {total_counts['verified']} verified, {total_counts['failed']} failed verification")
    print(f"📊 Total properties processed: {properties_verified} / {total_properties if total_properties else 'all'}")
    print(f"   Failure breakdown: {total_counts['square_footage']} square footage, "
         f"{total_counts['not_interested']} not interested, "
         f"{total_counts['no_address']} no address, "
         f"{total_counts['api_error']} API errors")

if __name__ == "__main__":
    main()