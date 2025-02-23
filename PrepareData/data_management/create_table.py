"""
This script establishes a connection to a PostgreSQL database
using configuration details loaded from a YAML file.
It then creates four tables: "market", "assets", "asset_type", and "raw_data".
The "market" table stores general market information,
the "assets" table stores information about individual assets (e.g., stocks),
the "asset_type" table stores the types of assets (eg.,Stock, ETF),
and the "raw_data" table stores raw data related to assets' trading history.
After creating the tables, the script commits the changes
and closes the connection.
"""

import os
import psycopg2
import yaml

current_directory = os.getcwd()
print("Current working directory:", current_directory)


def load_config(filename):
    """
    Method to load config file
    """
    with open(filename, "r", encoding="utf-8") as file:
        configs = yaml.safe_load(file)
    return configs


# Load database configuration from config.yml
config = load_config("../config.yml")

# Connect to PostgreSQL using configuration from config.yml
conn = psycopg2.connect(
    dbname=config["database"]["dbname"],
    user=config["database"]["user"],
    password=config["database"]["password"],
    host=config["database"]["host"],
)

# Create cursor
cursor = conn.cursor()

# Create market table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS market (
    market_id SERIAL PRIMARY KEY,
    market_short_name VARCHAR(255),
    market_long_name VARCHAR(255),
    currency VARCHAR(255),
    country VARCHAR(255),
    last_aggregator_update TIMESTAMP,
    timestamp TIMESTAMP 
)
"""
)

# Create asset_type table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS asset_type (
    id SERIAL PRIMARY KEY,
    asset_type varchar(255),
    volume_unit TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
)

# Insert types 'Stock' and 'ETF' into asset_type table
cursor.execute("INSERT INTO asset_type (asset_type) VALUES ('Stock'), ('ETF')")

# Create company table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(255) UNIQUE,
    name VARCHAR(255),
    market INT,
    asset_type INT,
    sector VARCHAR(255),
    logo_url TEXT,
    ipo_date DATE,
    delisting_date DATE,
    market_cap FLOAT,
    status VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market) REFERENCES market(market_id),
    FOREIGN KEY (asset_type) REFERENCES asset_type(id)
)
"""
)

# # Create raw_data table
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS raw_data (
#     primary_key SERIAL PRIMARY KEY,
#     market INT,
#     stock INT,
#     date DATE,
#     open FLOAT,
#     high FLOAT,
#     low FLOAT,
#     close FLOAT,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     FOREIGN KEY (market) REFERENCES market(market_id),
#     FOREIGN KEY (stock) REFERENCES assets(id)
# )
# """)

# Create daily_data table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS daily_data (
    id SERIAL PRIMARY KEY,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    id_asset INT,
    wise TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_asset) REFERENCES assets(id)
)
"""
)
# FOREIGN KEY (id_market) REFERENCES market(market_id),
# UNIQUE (id_asset)

# Create user table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255),
    name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    phone NUMERIC,
    firebase_uid VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE
)
"""
)

# Create watchlist table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS watchlist (
    id SERIAL PRIMARY KEY,
    user_id INT,
    watchlist_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    is_watchlist BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
"""
)

# Create watchlist_item table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS watchlist_item (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(255),
    symbol_id INT,
    watchlist_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (symbol) REFERENCES assets(ticker),
    FOREIGN KEY (symbol_id) REFERENCES assets(id),
    FOREIGN KEY (watchlist_id) REFERENCES watchlist(id)
)
"""
)

# Create streaks table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS streaks (
    id SERIAL PRIMARY KEY,
    start_date DATE,
    end_date DATE,
    symbol_id INT,
    streak_len INT,
    streak_gain FLOAT,
    streak_vol FLOAT,
    streak_df_close FLOAT[],
    wise TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES assets(id)
)
"""
)

# Create aggregator_streaks table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS streaks (
    id SERIAL PRIMARY KEY,
    start_date DATE,
    end_date DATE,
    symbol_id INT,
    streak_len INT,
    streak_gain FLOAT,
    streak_vol FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES assets(id)
)
"""
)

# Create streaks_running_summary
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS streaks_running_summary (
    id SERIAL PRIMARY KEY,
    streak_len INT,
    occurrences_count INT,
    gain_max FLOAT,
    gain_mean FLOAT,
    gain_running FLOAT,
    vol_max FLOAT,
    vol_mean FLOAT,
    vol_running FLOAT,
    gain_min FLOAT,
    gain_median FLOAT,
    gain_std FLOAT,
    gain_max_date DATE,
    vol_min FLOAT,
    vol_median FLOAT,
    vol_std FLOAT,
    vol_max_date DATE,
    end_date DATE,
    start_date DATE,
    streak_df_close FLOAT[],
    symbol VARCHAR(255), 
    symbol_id INT,
    name_asset TEXT
    )
    """
)


# Create last_aggregator_update
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS last_aggregator_update (
    id SERIAL PRIMARY KEY,
    market_id INT,
    wise TEXT,
    date DATE,
    FOREIGN KEY (market_id) REFERENCES market(market_id),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
)


# Create streaks_weekly table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS streaks_weekly (
    id SERIAL PRIMARY KEY,
    start_date DATE,
    end_date DATE,
    symbol_id INT,
    streak_len INT,
    streak_gain FLOAT,
    streak_vol FLOAT,
    streak_df_close FLOAT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES assets(id)
)
"""
)

# Create aggregator_streaks_weekly table
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS aggregator_streaks_weekly (
    id SERIAL PRIMARY KEY,
    start_date DATE,
    end_date DATE,
    symbol_id INT,
    streak_len INT,
    streak_gain FLOAT,
    streak_vol FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES assets(id)
)
"""
)

# Create streaks_running_summary_weekly
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS streaks_running_summary_weekly (
    id SERIAL PRIMARY KEY,
    streak_len INT,
    occurrences_count INT,
    gain_max FLOAT,
    gain_mean FLOAT,
    gain_running FLOAT,
    vol_max FLOAT,
    vol_mean FLOAT,
    vol_running FLOAT,
    gain_min FLOAT,
    gain_median FLOAT,
    gain_std FLOAT,
    gain_max_date DATE,
    vol_min FLOAT,
    vol_median FLOAT,
    vol_std FLOAT,
    vol_max_date DATE,
    end_date DATE,
    start_date DATE,
    streak_df_close FLOAT[],
    symbol VARCHAR(255), 
    symbol_id INT,
    name_asset TEXT
    )
    """
)
# Commit changes
conn.commit()

cursor.execute(
        """
        DO $$
DECLARE
    json_data json := '{"id":{"0":60,"1":49,"2":2,"3":1},"asset_type":{"0":"Crypto","1":"Index","2":"ETF","3":"Stock"},"timestamp":{"0":1729152796343,"1":1721040063711,"2":1725892731400,"3":1725892731400},"volume_unit":{"0":"USD","1":"SHR","2":"SHR","3":"SHR"}}';
 

    key TEXT;
    id_value INT;
    asset_value TEXT;
    timestamp_value BIGINT;
BEGIN
    -- Iterate through JSON keys (indices)
    FOR key IN SELECT json_object_keys(json_data->'id')
    LOOP
        -- Extract values for each key
        id_value := (json_data->'id'->>key)::INT;
        asset_value := json_data->'asset_type'->>key;
        timestamp_value := (json_data->'timestamp'->>key)::BIGINT;

        -- Insert or update the table, converting timestamp_value to timestamp
        INSERT INTO asset_type (id, asset_type, timestamp)
        VALUES (id_value, asset_value, to_timestamp(timestamp_value / 1000.0))
        ON CONFLICT (id)
        DO UPDATE SET
            asset_type = EXCLUDED.asset_type,
            timestamp = EXCLUDED.timestamp;
    END LOOP;
END $$;

"""
)

conn.commit()

# Close cursor and connection
cursor.close()
conn.close()

