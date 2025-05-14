# Real Estate Data System

This project contains scripts for scraping real estate data from multiple sources (Realtor.com, Zillow, and Redfin), storing the data in a MySQL database, and a web interface for managing the data collection and verification process.

## Features

- Scrapes property listings from Realtor.com, Zillow, and Redfin
- Stores data in a centralized MySQL database
- Verifies properties against SFR3 API and business rules
- Handles duplicate detection and avoids re-inserting existing properties
- Includes rate limiting and random delays to avoid blocking
- Tracks verification status for properties including failure reasons
- Provides a web dashboard for monitoring and controlling the system
- Generates visualizations of property data and verification statistics

## Setup

1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure the database connection by editing the `config.env` file:
   ```
   DB_HOST=162.241.217.138
   DB_USER=lqtincco_talal
   DB_PASSWORD=2105264@tT
   DB_NAME=lqtincco_sfr3
   ```

## Running the Web Application

Start the web application with:

```
python app.py
```

Then visit `http://localhost:5000` in your web browser to access the dashboard.

The web interface has three main sections:
1. **Dashboard** - Overview of all properties and verification status
2. **Scrapers** - Run and monitor property data collection
3. **Verification** - Run property verification and download verified data

## Running the Scrapers

The scrapers can be run through the web interface or independently:

### Zillow Scraper

```
python zillow_db.py
```

### Realtor Scraper

```
python realtor_db.py
```

### Redfin Scraper

```
python redfin_db.py
```

## Running the Property Verification

The property verification can be run through the web interface or directly:

```
python sfr3_checker.py
```

## Database Structure

The scrapers will automatically create the necessary database table if it doesn't exist. The table schema is as follows:

```sql
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
    bathrooms FLOAT,
    year_built INT,
    after_repair_value FLOAT,
    url TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    failure_reason VARCHAR(50) NULL,
    source VARCHAR(50),
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## Verification Process

All new properties are initially added with `is_verified = FALSE`. The verification process checks:

1. **Square Footage** - Must be at least 800 square feet
2. **SFR3 API** - Address must be checked against the SFR3 API

Failure reasons are tracked as:
- **SQUARE_FOOTAGE** - Failed due to insufficient square footage
- **NOT_INTERESTED** - SFR3 API returned "not interested"
- **NO_ADDRESS** - Property lacks an address for verification
- **API_ERROR** - Temporary API error, will be retried

## Data Sources

The scrapers collect data from the following sources:

- **Zillow**: Single-family and multi-family properties in 13 states
- **Realtor.com**: Single-family and multi-family properties in 13 states
- **Redfin**: Properties in 13 states

## States Covered

- Alabama
- Georgia
- Indiana
- Kansas
- Kentucky
- Missouri
- North Carolina
- Ohio
- South Carolina
- Tennessee
- Arkansas
- Wisconsin
- Michigan 