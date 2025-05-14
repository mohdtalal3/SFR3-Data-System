import mysql.connector
import os
from dotenv import load_dotenv
import logging
from mysql.connector import pooling

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

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '12345678'),
    'database': os.getenv('DB_NAME', 'lqtincco_sfr3')
}

# Connection pool
CONNECTION_POOL = None

# Define the table schema
CREATE_TABLE_QUERY = """
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
"""

def initialize_connection_pool(pool_size=5):
    """Initialize the connection pool with specified size."""
    global CONNECTION_POOL
    if CONNECTION_POOL is None:
        try:
            CONNECTION_POOL = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="sfr3_pool",
                pool_size=pool_size,
                **DB_CONFIG
            )
            logger.info(f"Database connection pool initialized with size {pool_size}")
        except mysql.connector.Error as err:
            logger.error(f"Error initializing MySQL connection pool: {err}")
            raise

def get_db_connection():
    """Get a connection from the pool or create a new one if pool is not initialized."""
    global CONNECTION_POOL
    
    # Initialize pool if not done already
    if CONNECTION_POOL is None:
        initialize_connection_pool()
    
    try:
        connection = CONNECTION_POOL.get_connection()
        if not connection.is_connected():
            connection.reconnect()
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Error getting connection from pool: {err}")
        # Fallback to direct connection if pool fails
        try:
            connection = mysql.connector.connect(**DB_CONFIG)
            logger.info("Database connection established (fallback)")
            return connection
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to MySQL database: {err}")
            raise

def create_tables():
    """Create the necessary tables if they don't exist."""
    connection = get_db_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(CREATE_TABLE_QUERY)
        connection.commit()
        logger.info("Tables created or already exist")
        
        # Check if failure_reason column exists and add it if it doesn't
        try:
            cursor.execute("SHOW COLUMNS FROM properties LIKE 'failure_reason'")
            column_exists = cursor.fetchone() is not None
            
            if not column_exists:
                # Add the column
                cursor.execute("ALTER TABLE properties ADD COLUMN failure_reason VARCHAR(50) NULL")
                connection.commit()
                logger.info("Added failure_reason column to properties table")
        except mysql.connector.Error as err:
            logger.error(f"Error checking/adding failure_reason column: {err}")
            
        # Force modify bathrooms column type to DOUBLE regardless of current type
        try:
            # First check current type
            cursor.execute("SHOW COLUMNS FROM properties LIKE 'bathrooms'")
            column_info = cursor.fetchone()
            
            # Always modify to ensure it's DOUBLE
            cursor.execute("ALTER TABLE properties MODIFY COLUMN bathrooms DOUBLE")
            connection.commit()
            logger.info("Ensured bathrooms column type is DOUBLE")
        except mysql.connector.Error as err:
            logger.error(f"Error modifying bathrooms column: {err}")
            
    except mysql.connector.Error as err:
        logger.error(f"Error creating tables: {err}")
        raise
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def property_exists(property_id, connection=None):
    """Check if a property with the given ID already exists in the database."""
    close_connection = False
    if not connection:
        connection = get_db_connection()
        close_connection = True
        
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
        if close_connection and connection.is_connected():
            cursor.close()
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
    
    try:
        # Check if property already exists
        if property_exists(formatted_data['property_id'], connection):
            logger.info(f"Property {formatted_data['property_id']} already exists, skipping")
            return False
        
        cursor = connection.cursor()
        
        # Prepare the SQL query
        insert_query = """
        INSERT INTO properties
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
        if connection.is_connected():
            cursor.close()
            connection.close()

def batch_insert_properties(properties_list, source):
    """Insert multiple properties efficiently in batches."""
    if not properties_list:
        return 0, 0
    
    # Simplify source name by removing property type suffix if present
    simplified_source = source.split('-')[0] if '-' in source else source
    
    connection = get_db_connection()
    
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
                INSERT INTO properties
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
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_verification_status(property_id, is_verified=True):
    """Update the verification status of a property."""
    connection = get_db_connection()
    
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
        if connection.is_connected():
            cursor.close()
            connection.close()

# Initialize the connection pool and database when module is imported
try:
    initialize_connection_pool(pool_size=30)
    create_tables()
except Exception as e:
    logger.error(f"Failed to initialize database: {e}") 