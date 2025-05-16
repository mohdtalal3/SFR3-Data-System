import mysql.connector
import os
from dotenv import load_dotenv
import logging
from mysql.connector import pooling
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_operations.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_connector")

# Load environment variables from .env file
load_dotenv()

# Global variables
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Global connection and pool
global_connection = None
connection_pool = None

def initialize_db():
    """Initialize the global database connection when the app starts.
    
    This should be called once during application startup.
    """
    global global_connection, connection_pool
    
    try:
        # First try connecting directly without a pool for the global connection
        logger.info("Initializing global database connection...")
        global_connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            autocommit=False  # We want to control transactions manually
        )
        
        # Test the connection
        cursor = global_connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        
        logger.info("Global database connection established successfully")
        
        # Create a small pool for additional connections when absolutely needed
        # This helps with concurrent operations while keeping total connections low
        pool_size = 5  # Reduced from default 30+
        logger.info(f"Initializing database connection pool with size {pool_size}")
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="app_pool",
            pool_size=pool_size,
            **db_config
        )
        logger.info(f"Database connection pool initialized with size {pool_size}")
        
        # Initialize database tables
        _initialize_tables()
        
        return True
    except mysql.connector.Error as err:
        logger.error(f"Error initializing MySQL connection pool: {err}")
        logger.error(f"Failed to initialize database: {err}")
        return False

def get_db_connection():
    """Get the global database connection.
    
    This will return the global connection if it's active,
    or try to reconnect if it's disconnected.
    """
    global global_connection
    
    # First, check if the global connection exists and is connected
    if global_connection and global_connection.is_connected():
        return global_connection
    
    # If global connection is not connected, try to reconnect
    logger.info("Global database connection is not available, attempting to reconnect...")
    try:
        if global_connection:
            # Try to close the old connection first to avoid lingering connections
            try:
                global_connection.close()
            except:
                pass
        
        # Create a new connection
        global_connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            autocommit=False
        )
        
        # Test the connection
        cursor = global_connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        
        logger.info("Global database connection reestablished successfully")
        return global_connection
    except mysql.connector.Error as err:
        logger.error(f"Error reconnecting to database: {err}")
        
        # As a last resort, try to get a connection from the pool
        if connection_pool:
            try:
                logger.info("Attempting to get connection from pool as fallback...")
                return connection_pool.get_connection()
            except mysql.connector.Error as pool_err:
                logger.error(f"Error getting connection from pool: {pool_err}")
        
        return None

def get_new_connection_from_pool():
    """Get a new connection from the pool when a separate connection is absolutely necessary.
    
    This should be used sparingly for operations that need to be isolated from the main connection.
    """
    global connection_pool
    
    if connection_pool:
        try:
            return connection_pool.get_connection()
        except mysql.connector.Error as err:
            logger.error(f"Error getting connection from pool: {err}")
    
    logger.error("Connection pool is not initialized")
    return None

def close_db_connection():
    """Close the global database connection.
    
    This should be called when shutting down the application.
    """
    global global_connection
    
    if global_connection:
        try:
            if global_connection.is_connected():
                global_connection.close()
                logger.info("Global database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing global database connection: {e}")
        finally:
            global_connection = None

def _initialize_tables():
    """Initialize the required database tables if they don't exist."""
    if not global_connection or not global_connection.is_connected():
        logger.error("Cannot initialize tables: No database connection")
        return False
    
    try:
        cursor = global_connection.cursor()
        
        # Create properties table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                id INT AUTO_INCREMENT PRIMARY KEY,
                property_id VARCHAR(255) UNIQUE,
                state VARCHAR(100),
                property_type VARCHAR(100),
                occupancy_status VARCHAR(50),
                address TEXT,
                zip_code VARCHAR(20),
                square_footage FLOAT,
                bedrooms INT,
                bathrooms DOUBLE,
                year_built INT,
                after_repair_value FLOAT,
                url TEXT,
                is_verified BOOLEAN DEFAULT FALSE,
                failure_reason VARCHAR(50) NULL,
                source VARCHAR(50),
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
        # Ensure bathrooms column is DOUBLE type
        try:
            cursor.execute("ALTER TABLE properties MODIFY bathrooms DOUBLE")
            logger.info("Ensured bathrooms column type is DOUBLE")
        except mysql.connector.Error as err:
            # If the alteration fails, it's likely already the correct type
            pass
        
        global_connection.commit()
        logger.info("Tables created or already exist")
        cursor.close()
        return True
            
    except mysql.connector.Error as err:
        logger.error(f"Error initializing tables: {err}")
        return False

# Legacy function for backward compatibility
def get_db_connection_from_pool():
    """Legacy function that now returns the global connection or a pool connection as fallback.
    
    This maintains backward compatibility with code that expects a connection from the pool.
    """
    conn = get_db_connection()
    if conn:
        return conn
    
    # If global connection fails, try pool as fallback
    return get_new_connection_from_pool()

def property_exists(property_id, connection=None):
    """Check if a property with the given ID already exists in the database."""
    close_connection = False
    if not connection:
        connection = get_db_connection()
        close_connection = True  # Only close if we created a new connection
        
    if not connection:
        logger.error("Cannot check if property exists: No database connection")
        return False if not isinstance(property_id, list) else {}
        
    try:
        cursor = connection.cursor()
        
        # Handle both single ID and list of IDs
        if isinstance(property_id, list):
            if not property_id:  # Empty list
                return {}
                
            placeholders = ', '.join(['%s'] * len(property_id))
            check_query = f"SELECT property_id FROM properties WHERE property_id IN ({placeholders})"
            cursor.execute(check_query, property_id)
            
            # Return a set of existing IDs
            return {row[0] for row in cursor.fetchall()}
        else:
            # Single ID check
            check_query = "SELECT COUNT(*) FROM properties WHERE property_id = %s"
            cursor.execute(check_query, (property_id,))
            result = cursor.fetchone()
            return result[0] > 0
            
    except mysql.connector.Error as err:
        logger.error(f"Error checking for existing property: {err}")
        return {} if isinstance(property_id, list) else False
    finally:
        if cursor:
            cursor.close()
        # Only close the connection if we created it
        if close_connection and connection and hasattr(connection, 'is_connected') and connection.is_connected():
            connection.close()

def insert_property(property_data, source):
    """Insert a new property into the database if it doesn't already exist."""
    # Format property data to match database schema
    formatted_data = {
        'property_id': property_data.get('property_id', ''),
        'state': property_data.get('State', ''),
        'property_type': property_data.get('Formatted Property Type', ''),
        'occupancy_status': property_data.get('Occupied/Vacant', 'Unknown'),
        'address': property_data.get('Address', ''),
        'zip_code': property_data.get('Zip Code', ''),
        'square_footage': property_data.get('Square Footage', 0),
        'bedrooms': property_data.get('Rooms (Beds)', 0),
        'bathrooms': property_data.get('Bathrooms', 0),
        'year_built': property_data.get('Year Built', 0),
        'after_repair_value': property_data.get('After Repair Value', 0),
        'url': property_data.get('URL', ''),
        'is_verified': False,
        'source': source.split('-')[0] if '-' in source else source  # Simplify source name
    }
    
    # Skip if property_id is empty
    if not formatted_data['property_id']:
        logger.warning("Skipping property with empty ID")
        return False
    
    connection = get_db_connection()
    if not connection:
        logger.error("Cannot insert property: No database connection")
        return False
    
    try:
        # Check if property already exists
        if property_exists(formatted_data['property_id'], connection):
            logger.info(f"Property {formatted_data['property_id']} already exists, skipping")
            return False
        
        cursor = connection.cursor()
        
        # Prepare the SQL query
        insert_query = """
        INSERT IGNORE INTO properties
        (property_id, state, property_type, occupancy_status, address, zip_code, 
        square_footage, bedrooms, bathrooms, year_built, after_repair_value, url, is_verified, source)
        VALUES
        (%(property_id)s, %(state)s, %(property_type)s, %(occupancy_status)s, %(address)s, %(zip_code)s,
        %(square_footage)s, %(bedrooms)s, %(bathrooms)s, %(year_built)s, %(after_repair_value)s, %(url)s, %(is_verified)s, %(source)s)
        """

        
        cursor.execute(insert_query, formatted_data)
        connection.commit()
        logger.info(f"Successfully inserted property {formatted_data['property_id']} from {formatted_data['source']}")
        return True
        
    except mysql.connector.Error as err:
        logger.error(f"Error inserting property: {err}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        # Don't close the connection since we're using a singleton pattern

def batch_insert_properties(properties_list, source):
    """Insert multiple properties efficiently in batches."""
    if not properties_list:
        return 0, 0
    
    # Simplify source name by removing property type suffix if present
    simplified_source = source.split('-')[0] if '-' in source else source
    
    connection = get_db_connection()
    if not connection:
        logger.error("Cannot insert properties: No database connection")
        return 0, 0
    
    try:
        cursor = connection.cursor()
        
        # Get all existing property IDs in one query
        property_ids = [prop.get('property_id', '') for prop in properties_list if prop.get('property_id', '')]
        if not property_ids:
            return 0, 0
            
        placeholders = ', '.join(['%s'] * len(property_ids))
        check_query = f"SELECT property_id FROM properties WHERE property_id IN ({placeholders})"
        cursor.execute(check_query, property_ids)
        existing_ids = {row[0] for row in cursor.fetchall()}
        
        # Prepare batch data for insertion
        insert_data = []
        skipped_count = 0
        
        for property_data in properties_list:
            property_id = property_data.get('property_id', '')
            
            # Skip if property_id is empty or already exists
            if not property_id or property_id in existing_ids:
                skipped_count += 1
                continue
                
            # Format property data with extra validation for bathrooms
            bathroom_value = property_data.get('Bathrooms', 0)
            # Ensure bathroom value is a valid float/double
            try:
                bathroom_value = float(bathroom_value)
            except (ValueError, TypeError):
                bathroom_value = 0.0
                
            formatted_data = {
                'property_id': property_id,
                'state': property_data.get('State', ''),
                'property_type': property_data.get('Formatted Property Type', ''),
                'occupancy_status': property_data.get('Occupied/Vacant', 'Unknown'),
                'address': property_data.get('Address', ''),
                'zip_code': property_data.get('Zip Code', ''),
                'square_footage': property_data.get('Square Footage', 0),
                'bedrooms': property_data.get('Rooms (Beds)', 0),
                'bathrooms': bathroom_value,
                'year_built': property_data.get('Year Built', 0),
                'after_repair_value': property_data.get('After Repair Value', 0),
                'url': property_data.get('URL', ''),
                'is_verified': False,
                'source': simplified_source
            }
            
            insert_data.append(formatted_data)
        
        # Use bulk insert
        inserted_count = 0
        if insert_data:
            # Increase batch size to 1000 or even higher since these are small records
            batch_size = 1000
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i+batch_size]
                
                # Prepare the SQL query for batch
                insert_query = """
                INSERT IGNORE INTO properties
                (property_id, state, property_type, occupancy_status, address, zip_code, 
                square_footage, bedrooms, bathrooms, year_built, after_repair_value, url, is_verified, source)
                VALUES
                (%(property_id)s, %(state)s, %(property_type)s, %(occupancy_status)s, %(address)s, %(zip_code)s,
                %(square_footage)s, %(bedrooms)s, %(bathrooms)s, %(year_built)s, %(after_repair_value)s, %(url)s, %(is_verified)s, %(source)s)
"""

                
                try:
                    # Execute batch insert
                    cursor.executemany(insert_query, batch)
                    batch_count = cursor.rowcount
                    inserted_count += batch_count
                    
                    # Commit each batch 
                    connection.commit()
                    logger.info(f"Committed batch of {batch_count} properties from {simplified_source}, total so far: {inserted_count}")
                    
                except mysql.connector.Error as err:
                    logger.error(f"Error in batch: {err}")
                    connection.rollback()
        
        logger.info(f"Batch insert complete: {inserted_count} inserted, {skipped_count} skipped from {simplified_source}")
        return inserted_count, skipped_count
        
    except mysql.connector.Error as err:
        logger.error(f"Error in batch insertion: {err}")
        connection.rollback()
        return 0, 0
    finally:
        if cursor:
            cursor.close()
        # Don't close the connection since we're using a singleton pattern

def update_verification_status(property_id, is_verified=True):
    """Update the verification status of a property."""
    connection = get_db_connection()
    if not connection:
        logger.error(f"Cannot update verification status for {property_id}: No database connection")
        return False
    
    try:
        cursor = connection.cursor()
        update_query = "UPDATE properties SET is_verified = %s WHERE property_id = %s"
        cursor.execute(update_query, (is_verified, property_id))
        connection.commit()
        
        if cursor.rowcount > 0:
            logger.info(f"Successfully updated verification status for property {property_id}")
            return True
        else:
            logger.warning(f"No property found with ID {property_id}")
            return False
            
    except mysql.connector.Error as err:
        logger.error(f"Error updating verification status: {err}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        # Don't close the connection since we're using a singleton pattern

# Function for scrapers to call for table creation
def create_tables():
    """
    Legacy function for scrapers to call before inserting data.
    This now just uses the internal _initialize_tables function.
    """
    logger.info("Ensuring database tables exist (called from scraper)")
    return _initialize_tables()

# Initialize the connection pool and database when module is imported
try:
    initialize_db()
except Exception as e:
    logger.error(f"Failed to initialize database: {e}") 