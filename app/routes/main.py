from flask import Blueprint, render_template
import db_connector
import mysql.connector

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    # Get database statistics
    connection = db_connector.get_db_connection()
    stats = {}
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Total properties
        cursor.execute("SELECT COUNT(*) as total FROM properties")
        stats['total_properties'] = cursor.fetchone()['total']
        
        # Verified properties
        cursor.execute("SELECT COUNT(*) as verified FROM properties WHERE is_verified = TRUE")
        stats['verified_properties'] = cursor.fetchone()['verified']
        
        # Properties by source
        cursor.execute("SELECT source, COUNT(*) as count FROM properties GROUP BY source")
        stats['properties_by_source'] = cursor.fetchall()
        
        # Properties by state
        cursor.execute("SELECT state, COUNT(*) as count FROM properties GROUP BY state ORDER BY count DESC")
        stats['properties_by_state'] = cursor.fetchall()
        
    except mysql.connector.Error as err:
        print(f"Error fetching stats: {err}")
        stats = {
            'total_properties': 0,
            'verified_properties': 0,
            'properties_by_source': [],
            'properties_by_state': []
        }
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    
    return render_template('index.html', stats=stats) 