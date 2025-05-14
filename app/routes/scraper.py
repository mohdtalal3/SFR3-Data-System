from flask import Blueprint, render_template, request, jsonify, redirect, url_for
import os
import sys
import time
import threading
import json
import importlib
import db_connector
import mysql.connector

# Import the scraper modules
import zillow_db
import realtor_db
import redfin_db

scraper_bp = Blueprint('scraper', __name__)

# Dictionary to track scraper status
scraper_status = {
    'zillow': {'running': False, 'progress': 0, 'total': 0, 'message': 'Idle'},
    'realtor': {'running': False, 'progress': 0, 'total': 0, 'message': 'Idle'},
    'redfin': {'running': False, 'progress': 0, 'total': 0, 'message': 'Idle'}
}

def run_scraper_thread(scraper_name, states=None):
    """Run a scraper in a background thread with progress tracking"""
    try:
        scraper_status[scraper_name]['running'] = True
        scraper_status[scraper_name]['progress'] = 0
        scraper_status[scraper_name]['message'] = 'Starting...'
        
        # Get the appropriate scraper module
        if scraper_name == 'zillow':
            scraper_module = zillow_db
        elif scraper_name == 'realtor':
            scraper_module = realtor_db
        elif scraper_name == 'redfin':
            scraper_module = redfin_db
        else:
            scraper_status[scraper_name]['message'] = f'Unknown scraper: {scraper_name}'
            scraper_status[scraper_name]['running'] = False
            return
        
        # Run the scraper's main function
        # We'll monkey patch the scraper's progress reporting
        import builtins
        original_print = builtins.print
        
        def custom_print(*args, **kwargs):
            message = ' '.join(str(arg) for arg in args)
            scraper_status[scraper_name]['message'] = message
            original_print(*args, **kwargs)
            
            # Try to parse progress information from the message
            if "Processing" in message and "of" in message:
                try:
                    parts = message.split()
                    current_idx = parts.index("Processing") + 1
                    total_idx = parts.index("of") + 1
                    
                    current = int(parts[current_idx])
                    total = int(parts[total_idx])
                    
                    scraper_status[scraper_name]['progress'] = current
                    scraper_status[scraper_name]['total'] = total
                except (ValueError, IndexError):
                    pass
        
        # Replace print function temporarily for progress tracking
        builtins.print = custom_print
        
        # Run the scraper with selected states
        original_print(f"Starting {scraper_name} scraper with states: {states if states else 'All'}")
        if states:
            scraper_module.main(states)
        else:
            scraper_module.main()
            
    except Exception as e:
        import builtins
        builtins.print(f"Scraper error: {str(e)}")
        scraper_status[scraper_name]['message'] = f'Error: {str(e)}'
    finally:
        scraper_status[scraper_name]['running'] = False
        scraper_status[scraper_name]['message'] = 'Completed'
        
        # Restore original print
        import builtins
        builtins.print = original_print

@scraper_bp.route('/')
def index():
    """Display scraper dashboard and status"""
    # Get scraper statistics from the database
    connection = db_connector.get_db_connection()
    stats = {}
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Properties by source
        cursor.execute("""
            SELECT source, COUNT(*) as count 
            FROM properties 
            GROUP BY source
        """)
        stats['properties_by_source'] = cursor.fetchall()
        
        # Properties by state and source
        cursor.execute("""
            SELECT state, source, COUNT(*) as count 
            FROM properties 
            GROUP BY state, source
            ORDER BY state, source
        """)
        stats['properties_by_state_source'] = cursor.fetchall()
        
    except mysql.connector.Error as err:
        print(f"Error fetching scraper stats: {err}")
        stats = {
            'properties_by_source': [],
            'properties_by_state_source': []
        }
    finally:
        if cursor:
            cursor.close()
        # Don't close the connection as it's a shared global connection
    
    return render_template('scraper/index.html', 
                          status=scraper_status,
                          stats=stats)

@scraper_bp.route('/start', methods=['POST'])
def start_scraper():
    """Start a scraper"""
    scraper_name = request.form.get('scraper')
    states = request.form.getlist('states')
    
    if scraper_name not in scraper_status:
        return jsonify({'success': False, 'message': f'Unknown scraper: {scraper_name}'})
    
    if scraper_status[scraper_name]['running']:
        return jsonify({'success': False, 'message': f'{scraper_name} scraper is already running'})
    
    # Start the scraper in a background thread
    thread = threading.Thread(
        target=run_scraper_thread, 
        args=(scraper_name, states if states else None)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True, 'message': f'Started {scraper_name} scraper'})

@scraper_bp.route('/status')
def get_status():
    """Get current status of all scrapers"""
    return jsonify(scraper_status)

@scraper_bp.route('/status/<scraper_name>')
def get_scraper_status(scraper_name):
    """Get current status of a specific scraper"""
    if scraper_name not in scraper_status:
        return jsonify({'success': False, 'message': f'Unknown scraper: {scraper_name}'})
    
    return jsonify(scraper_status[scraper_name]) 