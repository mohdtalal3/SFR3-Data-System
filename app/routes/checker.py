from flask import Blueprint, render_template, request, jsonify, send_file
import os
import sys
import threading
import time
import io
import csv
import mysql.connector
import pandas as pd
import db_connector
import sfr3_checker
import random

checker_bp = Blueprint('checker', __name__)

# Variable to track checker status
checker_status = {
    'running': False,
    'verified_count': 0,
    'failed_count': 0,
    'api_error_count': 0,
    'square_footage_count': 0,
    'not_interested_count': 0,
    'no_address_count': 0,
    'progress': 0,
    'total': 0,
    'message': 'Idle',
    'server_overload': False
}

def run_checker_thread(batch_size=50, source=None, include_failed=True, total_properties=None, api_delay=1):
    """Run the property checker in a background thread with progress tracking"""
    global checker_status
    
    try:
        checker_status['running'] = True
        checker_status['verified_count'] = 0
        checker_status['failed_count'] = 0
        checker_status['api_error_count'] = 0
        checker_status['square_footage_count'] = 0
        checker_status['not_interested_count'] = 0
        checker_status['no_address_count'] = 0
        checker_status['progress'] = 0
        checker_status['message'] = 'Starting property verification...'
        checker_status['server_overload'] = False
        
        # Get properties to check
        properties_verified = 0
        max_properties = total_properties if total_properties else float('inf')
        
        while properties_verified < max_properties:
            # Calculate current batch size
            remaining = max_properties - properties_verified
            current_batch_size = min(batch_size, remaining)
            
            # Get a batch of properties
            properties = sfr3_checker.get_properties_to_verify(current_batch_size, source, include_failed)
            
            if not properties:
                checker_status['message'] = 'No more properties found to verify'
                break
                
            # Update total count on first batch
            if properties_verified == 0:
                if total_properties:
                    checker_status['total'] = min(len(properties), total_properties)
                    checker_status['message'] = f'Found {len(properties)} properties, will verify up to {total_properties}'
                else:
                    checker_status['total'] = len(properties)
                checker_status['message'] = f'Found {len(properties)} properties to verify'
        
            # Process each property
            for idx, prop in enumerate(properties):
                checker_status['progress'] = properties_verified + idx + 1
                property_id = prop['property_id']
                
                checker_status['message'] = f'Checking property {property_id} ({properties_verified + idx + 1} of {total_properties if total_properties else "all"})'
                
                # Skip properties with permanent failure reasons
                if prop.get('failure_reason') and prop.get('failure_reason') != 'API_ERROR':
                    checker_status['message'] = f'Skipping property {property_id} with permanent failure reason: {prop.get("failure_reason")}'
                    time.sleep(0.5)  # Short delay for user interface
                    continue
                    
                # Get detailed property information
                property_details = sfr3_checker.get_property_details(property_id)
                
                if not property_details:
                    checker_status['message'] = f'Error: Could not retrieve details for property {property_id}'
                    checker_status['failed_count'] += 1
                    time.sleep(0.5)
                    continue
                    
                # Verify the property
                is_verified, failure_reason = sfr3_checker.verify_property(property_details)
                
                # Update the status in the database
                success = sfr3_checker.update_verification_status(property_id, is_verified, failure_reason)
                
                if not success:
                    checker_status['message'] = f'Error updating verification status for property {property_id}'
                    time.sleep(0.5)
                    continue
                    
                # Update counts
                if is_verified:
                    checker_status['verified_count'] += 1
                else:
                    checker_status['failed_count'] += 1
                    
                    if failure_reason == 'API_ERROR':
                        checker_status['api_error_count'] += 1
                    elif failure_reason == 'SQUARE_FOOTAGE':
                        checker_status['square_footage_count'] += 1
                    elif failure_reason == 'NOT_INTERESTED':
                        checker_status['not_interested_count'] += 1
                    elif failure_reason == 'NO_ADDRESS':
                        checker_status['no_address_count'] += 1
                        
                # Throttle requests slightly to avoid overwhelming the API
                time.sleep(api_delay)
            
            # Update properties_verified count
            properties_verified += len(properties)
            
            # Check if we've reached the limit
            if properties_verified >= max_properties:
                checker_status['message'] = f'Completed verification of {properties_verified} properties (limit reached)'
                break
                
            # Get next batch if there are more properties to process and we haven't reached the limit
            if len(properties) < current_batch_size:
                checker_status['message'] = f'Completed verification of {properties_verified} properties (no more properties)'
                break
        
        if properties_verified == 0:
            checker_status['message'] = 'No properties found to verify'
        else:
            checker_status['message'] = f'Completed verification of {properties_verified} properties'
            
    except Exception as e:
        checker_status['message'] = f'Error: {str(e)}'
    finally:
        checker_status['running'] = False

@checker_bp.route('/')
def index():
    """Display checker dashboard and status"""
    # Get verification statistics from the database
    connection = db_connector.get_db_connection()
    stats = {}
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Verification status
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN is_verified = TRUE THEN 1 ELSE 0 END) as verified_count,
                SUM(CASE WHEN is_verified = FALSE THEN 1 ELSE 0 END) as unverified_count,
                COUNT(*) as total_count
            FROM properties
        """)
        stats['verification_status'] = cursor.fetchone()
        
        # Failure reasons
        cursor.execute("""
            SELECT failure_reason, COUNT(*) as count 
            FROM properties 
            WHERE failure_reason IS NOT NULL 
            GROUP BY failure_reason
        """)
        stats['failure_reasons'] = cursor.fetchall()
        
        # Verification status by source
        cursor.execute("""
            SELECT 
                source,
                SUM(CASE WHEN is_verified = TRUE THEN 1 ELSE 0 END) as verified_count,
                SUM(CASE WHEN is_verified = FALSE THEN 1 ELSE 0 END) as unverified_count,
                COUNT(*) as total_count
            FROM properties
            GROUP BY source
        """)
        stats['verification_by_source'] = cursor.fetchall()
        
        # Verification status by state
        cursor.execute("""
            SELECT 
                state,
                SUM(CASE WHEN is_verified = TRUE THEN 1 ELSE 0 END) as verified_count,
                SUM(CASE WHEN is_verified = FALSE THEN 1 ELSE 0 END) as unverified_count,
                COUNT(*) as total_count
            FROM properties
            GROUP BY state
        """)
        stats['verification_by_state'] = cursor.fetchall()
        
    except mysql.connector.Error as err:
        print(f"Error fetching verification stats: {err}")
        stats = {
            'verification_status': {'verified_count': 0, 'unverified_count': 0, 'total_count': 0},
            'failure_reasons': [],
            'verification_by_source': [],
            'verification_by_state': []
        }
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return render_template('checker/index.html', 
                          status=checker_status,
                          stats=stats)

@checker_bp.route('/start', methods=['POST'])
def start_checker():
    """Start the property checker"""
    if checker_status['running']:
        return jsonify({'success': False, 'message': 'Property checker is already running'})
    
    # Get parameters from the form
    max_threads = int(request.form.get('max_threads', 10))
    source = request.form.get('source', None)
    include_failed = request.form.get('include_failed', 'true').lower() == 'true'
    total_properties = request.form.get('total_properties', None)
    api_delay = request.form.get('api_delay', '1')
    
    # Convert numeric parameters
    try:
        if total_properties and total_properties.strip():
            total_properties = int(total_properties)
        else:
            total_properties = None
            
        # Default API delay to 1 second if invalid
        api_delay = float(api_delay) if api_delay and api_delay.strip() else 1
        if api_delay < 0.1:  # Minimum delay of 0.1 second
            api_delay = 0.1
    except (ValueError, TypeError):
        total_properties = None
        api_delay = 1  # Default to 1 second
    
    # Start the SFR3 checker in a background thread
    thread = threading.Thread(
        target=run_checker_with_threading, 
        args=(source, include_failed, total_properties, max_threads, api_delay)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True, 'message': 'Started property verification'})

def run_checker_with_threading(source=None, include_failed=True, total_properties=None, max_threads=10, api_delay=1):
    """Run the property checker using the new threaded implementation"""
    global checker_status
    
    try:
        checker_status['running'] = True
        checker_status['verified_count'] = 0
        checker_status['failed_count'] = 0
        checker_status['api_error_count'] = 0
        checker_status['square_footage_count'] = 0
        checker_status['not_interested_count'] = 0
        checker_status['no_address_count'] = 0
        checker_status['progress'] = 0
        checker_status['message'] = 'Starting property verification...'
        checker_status['server_overload'] = False
        
        # Store the original sleep function to restore it later
        original_sleep = time.sleep
        
        # Wrap the time.sleep function to enforce our API delay
        def custom_sleep(seconds):
            # Only apply our custom delay for very short sleeps that are likely random delays
            if seconds < 0.5:  # If it's the small random delay in verify_property
                # Add a random component between 0 and 2 seconds to the api_delay
                random_delay = api_delay + random.uniform(0, 2)
                return original_sleep(random_delay)  # Use our configured delay instead
            return original_sleep(seconds)  # Otherwise use the original delay
            
        # Override the sleep function during verification
        time.sleep = custom_sleep
        
        # Prepare arguments for the sfr3_checker.py script
        args = []
        
        if max_threads:
            args.extend(['--threads', str(max_threads)])
        
        if source:
            args.extend(['--source', source])
        
        if total_properties:
            args.extend(['--limit', str(total_properties)])
            
        if not include_failed:
            args.append('--skip-failed')
        
        # Create command line arguments from the list
        cmd_args = sys.argv.copy()
        cmd_args[1:] = args  # Replace any existing args
        sys.argv = cmd_args
        
        # Set up tracking variables
        start_time = time.time()
        last_progress = 0
        
        # Create a status update function to track progress
        def update_status(batch_counts, properties_verified, total):
            nonlocal last_progress
            
            # Check if verification was stopped due to too many API errors
            if batch_counts.get('server_overload', False):
                error_message = batch_counts.get('stop_message', "Too many consecutive API errors")
                checker_status['message'] = f"⚠️ STOPPED: {error_message}. Properties processed before stopping are saved."
                checker_status['server_overload'] = True
                return
            
            # Check if we crossed a 100-property boundary
            crossed_hundred_mark = (last_progress // 100) < (properties_verified // 100)
            
            checker_status['verified_count'] += batch_counts['verified']
            checker_status['failed_count'] += batch_counts['failed']
            checker_status['api_error_count'] += batch_counts['api_error']
            checker_status['square_footage_count'] += batch_counts['square_footage']
            checker_status['not_interested_count'] += batch_counts['not_interested']
            checker_status['no_address_count'] += batch_counts['no_address']
            checker_status['progress'] = properties_verified
            
            if total:
                checker_status['total'] = total
                percentage = (properties_verified / total) * 100
                checker_status['message'] = f'Processing... {percentage:.1f}% complete ({properties_verified}/{total})'
            else:
                checker_status['message'] = f'Processing... {properties_verified} properties checked'
                
            # If we crossed a 100-property mark, pause for 5 seconds
            if crossed_hundred_mark and properties_verified >= 100:
                checker_status['message'] = f'Pausing for 5 seconds after processing {properties_verified} properties...'
                time.sleep(5)
                
            last_progress = properties_verified
        
        # Hook into the main verification function
        original_process_batch = sfr3_checker.process_verification_batch
        
        def process_batch_with_updates(*args, **kwargs):
            result = original_process_batch(*args, **kwargs)
            # Update status after each batch
            update_status(result, checker_status['progress'] + len(args[0]), checker_status['total'])
            return result
        
        # Replace the function with our tracking version
        sfr3_checker.process_verification_batch = process_batch_with_updates
        
        # Run the main function
        checker_status['message'] = f'Starting verification process with {api_delay}s API delay...'
        if total_properties:
            checker_status['total'] = total_properties
            
        sfr3_checker.main()
        
        # Restore the original function and sleep
        sfr3_checker.process_verification_batch = original_process_batch
        time.sleep = original_sleep
        
        # Calculate time taken
        time_taken = time.time() - start_time
        checker_status['message'] = f'Completed verification of {checker_status["progress"]} properties in {time_taken:.1f} seconds'
        
    except Exception as e:
        checker_status['message'] = f'Error: {str(e)}'
        # Make sure we restore the original sleep function
        if 'original_sleep' in locals():
            time.sleep = original_sleep
    finally:
        checker_status['running'] = False
        # One more safety check to restore original sleep function
        if 'original_sleep' in locals():
            time.sleep = original_sleep

@checker_bp.route('/status')
def get_status():
    """Get current status of the checker"""
    return jsonify(checker_status)

@checker_bp.route('/download')
def download_verified():
    """Download verified properties as CSV"""
    connection = db_connector.get_db_connection()
    
    try:
        # Create a Pandas DataFrame with the verified properties
        query = """
        SELECT property_id, state, property_type, occupancy_status, address, zip_code, 
               square_footage, bedrooms, bathrooms, year_built, 
               after_repair_value, url, source
        FROM properties
        WHERE is_verified = TRUE
        """
        
        df = pd.read_sql(query, connection)
        
        # Create a CSV string from the DataFrame
        csv_data = io.StringIO()
        df.to_csv(csv_data, index=False)
        
        # Create a response with the CSV file
        output = io.BytesIO()
        output.write(csv_data.getvalue().encode('utf-8'))
        output.seek(0)
        
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name='verified_properties.csv'
        )
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error downloading data: {str(e)}'})
    finally:
        if connection.is_connected():
            connection.close() 