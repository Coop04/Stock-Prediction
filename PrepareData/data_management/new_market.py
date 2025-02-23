# This script takes the input from the user for market_short_name,
# market_long_name, currency, country and creates a new market object for
# that in the table "market".

from datetime import datetime
from . import db

try:
    # Accept user input for market names
    market_short_name = input("Enter market short name: ").upper()
    market_long_name = input("Enter market full name: ")
    currency = input("Enter market Currency: ")
    country = input("Enter market Country: ")

    # Create SQL insert query
    query = f"""
        INSERT INTO market (market_short_name, market_long_name, currency, country, timestamp)
        VALUES ('{market_short_name}', '{market_long_name}', '{currency}', '{country}', CURRENT_TIMESTAMP)
    """

    # Execute the query without fetching results
    db.query_database(query, fetch=False)

    print(f"Market {market_short_name} created successfully!")

except Exception as e:
    print(f"Error creating new market: {e}")
