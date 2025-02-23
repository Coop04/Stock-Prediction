"""This module provides database functionalities"""

# This script's function here is to update the "raw_data" table with the latest
# market data for the assets in the "assets" table.  It reads a csv file
# cointaining the compact size data from alphavantage and updates the table
# with the latest incoming data.

# pylint: disable=all
# pylint: disable=line-too-long
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W0621 # disable general exception warning
# pylint: disable=W0718 # disable general exception warning
# pylint: disable=W1203 # disable lazy formating warning for readability

# from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, Integer
from sqlalchemy.orm import sessionmaker

# import psycopg2
import yaml

# import requests
import pandas as pd
import datetime

import logging

logger = logging.getLogger(__name__)
logger.debug("Starting streaks.py")


def load_config(filename):
    """To load config file"""
    with open(filename, "r", encoding="utf-8") as file:
        configs = yaml.safe_load(file)
    return configs


config = load_config("../config.yml")

cdb = config["database"]
connection_string = f'postgresql://{cdb["user"]}:{cdb["password"]}@{cdb["host"]}:{cdb["port"]}/{cdb["dbname"]}'

# Create an engine and a sessionmaker
engine = create_engine(
    connection_string, pool_size=5, max_overflow=4, pool_timeout=5, pool_recycle=2
)
Session = sessionmaker(bind=engine)


def SessionCreate():
    """Create db Session"""
    # engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def DfFromDb(query):
    """
    Read the query results directly to pandas dataframe

    Args:
        query (str) : SQL query to be executed.
    """
    # logger.debug(f"{query} : before engine create in DfFromDb")
    # engine = create_engine(connection_string)
    # logger.debug(f"{query} : after engine create in DfFromDb")
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logger.warning(f"{e}, query_database failed for {query}")
        return None

    # logger.debug(f"{query} : df fetched in DfFromDb")
    # return df


# Function to query the database
def query_database(query, fetch=True):
    """
    Query data base with the query and return results

    Args:
        query (str) : SQL query to be executed.
        fetch (Bool) :
                        True : Fetch the result
                        False: Execute / commit the SQL query

        Returns:
            result : result of the query

    """
    session = SessionCreate()
    result = None
    try:
        query = text(query)
        result = session.execute(query)
        if fetch:
            result = result.fetchall()
        else:
            result = session.commit()
    except Exception as e:
        session.rollback()
        logger.warning(f"{e}, query_database failed for {query}")
    finally:
        session.close()  # Close the session (returns the connection to the pool)
    return result


def TablesList():
    """
    Lists all the tables in the connected db
    """
    q = "SELECT table_name\nFROM information_schema.tables\nWHERE table_schema = 'public' AND table_type = 'BASE TABLE';\n"
    tablesList = DfFromDb(q)
    # tablesList = query_database(q)
    return tablesList


def ColumnsOfTable(tableName):
    """
    To see the column names of table
    Parameters:
        tableName (stre) : Name of the table
    Returns:
        Column names of the table
    """
    tableName = "'" + tableName + "'"
    schema_name = "'public'"
    q = f"SELECT column_name, data_type, character_maximum_length, is_nullable \
    FROM information_schema.columns WHERE table_name = {tableName} AND table_schema = {schema_name};"
    columnsOfTable = DfFromDb(q)
    return columnsOfTable


def ReadTable(tableName, limit="ALL", order="DESC", by="id"):
    """
    Given a table name reads the rows of the table from the bottom
    Parameters:
        tableName (str) : tableName
        limit (int) : Number of rows to be read
    Returns:
        DataFrame of the table
    """
    q = f"SELECT * \
        FROM {tableName} \
        ORDER BY {by} {order} \
        LIMIT {limit};"
    df = DfFromDb(q)
    return df


def ReadTableDailyData(symbol, limit=1, idAsset=None):
    """
    Reads daily data table from the bottom, if required.
    Parameters:
        symbol : str :

    """
    # tableName = "daily_data"
    # order="DESC"
    # by = "id"
    if limit is None:
        limit = "ALL"
    q = f"""
        SELECT dd.date, dd.close, dd.volume as vol, dd.open, dd.high, dd.low
        FROM daily_data dd
        JOIN assets a ON dd.id_asset = a.id
        WHERE a.ticker = '{symbol}'
        ORDER BY dd.id DESC
        LIMIT {limit};
        """
    df = DfFromDb(q)
    return df


def IdAsset(symbol):
    """
    ToDo:
        1. Think of adding market as an argument
    Return the symbol_id of the symbol
    Parameters
        symbol : str
    Returns
        symbol_id
    """
    idAsset = DfFromDb(f"SELECT id, ticker FROM assets WHERE ticker = '{symbol}';")
    if len(idAsset) == 1:
        idAsset = idAsset.iloc[0]["id"]
        return idAsset
    logger.warning("{symbol} not in the assets table or multiple entries")
    return None


def CloseLastExchange(exchange, wise=None):
    """Returns the last time aggregator update ran for the exchange and wise"""

    if wise is None:
        return None

    queryCloseLastExchange = f"SELECT date FROM last_aggregator_update WHERE market_id = (SELECT market_id FROM market WHERE market_short_name = '{exchange}' AND wise = '{wise}');"
    closeLastExchange = DfFromDb(queryCloseLastExchange)
    if len(closeLastExchange) != 1:
        logger.error("CloseLastExchange returned error")
        return None
    closeLastExchange = closeLastExchange.iloc[0]["date"]
    return closeLastExchange


def CloseLast(symbol, idAsset=None, wise="day"):
    """Returns the dataframe corresponding to last update"""
    if idAsset is None:
        idAsset = IdAsset(symbol)
    queryCloseLast = (
        f"SELECT * FROM daily_data WHERE id_asset = {idAsset} AND wise = '{wise}';"
    )
    closeLast = DfFromDb(queryCloseLast)
    return closeLast


# Assuming this method is no more needed
# def CloseLastSet(symbol, ds, idAsset=None):
#    """Set the latest close price in the daily_data table
#    This works if only if there is a row for the symbol in the daily_data table"""
#    if idAsset is None:
#        idAsset = IdAsset(symbol)
#    qCloseLastSet = f"""UPDATE daily_data \
#            SET date = '{ds['date']}', \
#            close = {ds['close']} \
#            WHERE id_asset = {idAsset};"""
#    query_database(qCloseLastSet, fetch=False)


def CloseLastInsert(symbol, ds, idAsset=None, wise="day"):
    """Insert / Append a new row for the symbol, after deleting the existing one, if it doesn't exist"""
    if idAsset is None:
        idAsset = IdAsset(symbol)
    qCloseLastDelete = f""" \
            DELETE FROM daily_data
            WHERE id_asset = {idAsset}
            AND wise = '{wise}';"""
    query_database(qCloseLastDelete, fetch=False)

    date = str(ds["date"])
    close = str(ds["close"])
    qCloseLastInsert = f""" \
            INSERT INTO daily_data (id_asset, date, close, wise) \
            VALUES ({idAsset}, '{date}', {close}, '{wise}');
            """
    query_database(qCloseLastInsert, fetch=False)


def Ds2DailyData(symbol, dsLatestDay, idAsset=None):
    """
    Replaces the latest date ochl data for the symbol for identifying if there was an adjustment.
    If there is an adjustment, all the streaks for the symbol needs to be recalculated and updated.

    symbol: str
    dsLatestDay: pandas Series
    """
    if idAsset is None:
        idAsset = IdAsset(symbol)
    dsLatestDay = dsLatestDay.copy()
    dsLatestDay["id_asset"] = idAsset
    dfLatestDay = dsLatestDay.to_frame().T
    dfLatestDay.to_sql(
        "daily_data",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        index_label="id_asset",
        dtype={"id_asset": Integer()},
    )


def Df2DailyData(df, symbol):
    """
    Method to insert daily data to daily_data table. Review if symbol_id is properly used.
    """
    if not isinstance(df, pd.DataFrame):
        logger.warning(f"{symbol} Empty dataframe, check symbol:")
        return None
    df2DailyData = df.copy()
    df2DailyData.sort_values("date", inplace=True)
    df2DailyData["id_asset"] = IdAsset(symbol)
    df2DailyData.to_sql("daily_data", engine, if_exists="append", index=False)
    return df2DailyData


def DeleteDataForSymbolFromDailyData(symbol, limit=None):
    """
    Method to delete daily data.
    """
    idAsset = IdAsset(symbol)
    if limit is None:
        query = f"DELETE FROM daily_data WHERE id_asset = {idAsset};"
    else:
        query = f"DELETE FROM daily_data WHERE id IN (\
                SELECT id FROM (\
                SELECT id FROM daily_data\
                WHERE id_asset = {idAsset}\
                ORDER BY id DESC\
                LIMIT {limit}\
                ) subquery\
                );"
    result = query_database(query, fetch=False)
    return result


def Streak2StreaksTable(streak, wise="day"):
    """
    Insert / update a streak to the database. If the db already has a streak with same symbol and stating date, then the old streak is deleted and new one is inserted.

    """
    if isinstance(streak, pd.DataFrame):
        # if type(streak) == pd.DataFrame:
        for k in range(len(streak)):
            # print(k, "/", len(streak))
            query2Delete = f"""
                DELETE FROM streaks
                WHERE symbol_id = {streak.iloc[k]["symbol_id"]} AND start_date = DATE '{streak.iloc[k]["start_date"]}'
                AND wise = '{wise}';
                """
            query_database(query2Delete, fetch=False)
        streak["wise"] = wise
        streak.to_sql("streaks", engine, if_exists="append", index=False)
        return streak
    logger.warning(
        f"{streaks}, method db.Streaks2StreaksTable only works with pandas dataframe"
    )
    return None


def Streaks2StreaksTable(streaks, wise="day"):
    """
    Delete all streaks of the symbol in streaks and insert the updates streaks to the database.
    """
    if isinstance(streaks, pd.DataFrame):
        # if type(streaks) == pd.DataFrame:
        if len(streaks) != 0:
            query2Delete = f"""
                DELETE FROM streaks
                WHERE symbol_id = {streaks.iloc[0]["symbol_id"]}
                AND wise = '{wise}';
                """
            query_database(query2Delete, fetch=False)
            streaks["wise"] = wise
            streaks.to_sql("streaks", engine, if_exists="append", index=False)
        else:
            print(
                "streaks to write to database has no rows"
            )  # , symbol id is:", symbol_id)
        return streaks
    logger.warning("method db Streak2StreaksTable only works with pandas dataframe")
    return None


def DeleteAllStreaksForSymbolId(symbol_id, wise="day"):
    query2Delete = f"""
        DELETE FROM streaks
        WHERE symbol_id = {symbol_id} 
        AND wise = '{wise}';
        """
    query_database(query2Delete, fetch=False)


def DeleteExchangeFromStreaksRunningSummaryDb(exchange=None, wise=None):
    """Method to delte steaks from streaks_running_summary for an exchange"""
    if wise is None:
        raise ("Mention wise for delete")

    qDeleteExchangeFromDb = f"DELETE FROM streaks_running_summary \
        WHERE symbol_id IN ( \
            SELECT streaks_running_summary.symbol_id \
            FROM streaks_running_summary \
            JOIN assets ON streaks_running_summary.symbol_id = assets.id \
            JOIN market ON assets.market = market.market_id \
            WHERE market.market_short_name = '{exchange}' \
            AND streaks_running_summary.wise = '{wise}' \
        );"

    """
    qDeleteExchangeFromDb = f"DELETE FROM streaks_running_summary \
            JOIN assets ON streaks_running_summary.symbol_id = assets.id \
            JOIN market ON assets.market = market.market_id \
            WHERE market.market_short_name = '{exchange}' \
            ;"
    """
    query_database(qDeleteExchangeFromDb, fetch=False)


def ReadRunningStreaks(direction="", exchange=None, wise="day"):
    """
    Read running streaks from db
    """
    queryDateLatest = f"SELECT date FROM daily_data \
            JOIN assets ON daily_data.id_asset = assets.id \
            JOIN market ON assets.market = market.market_id \
            WHERE market.market_short_name = '{exchange}' \
            AND daily_data.wise = '{wise}' \
            ORDER BY date DESC LIMIT 1 \
            ;"
    logger.debug("queryDateLatest: %s , wise: %s", queryDateLatest, wise)
    dateLatest = DfFromDb(queryDateLatest)
    if len(dateLatest) == 1:
        dateLatest = dateLatest.iloc[0]["date"]
        logger.info("dateLatest = %s", dateLatest)
    else:
        logger.error("Empty or corrupted table streaks, dateLatest= %s", dateLatest)
        return None
    if direction == "up":
        compare = ">"
    elif direction == "down":
        compare = "<"
    else:
        # print(40 * "-", "Direction not set properly in db.py for ReadRunningStreak", 40 * "-")
        logger.error("%s Direction not set for ReadingRunningStreaks", exchange)
        return None
    queryStreaksLatest = f""" 
        SELECT s.*, a.ticker AS symbol, a.name AS name_asset \
        FROM streaks s \
        JOIN assets a ON s.symbol_id = a.id \
        JOIN market ON a.market = market.market_id \
        WHERE s.end_date = '{dateLatest}' \
        AND s.streak_gain {compare} 0 \
        AND market.market_short_name = '{exchange}' \
        AND s.wise = '{wise}' \
        ;"""
    streaksLatest = DfFromDb(queryStreaksLatest)
    return streaksLatest


def ReadStreaksForSymbol(
    symbol, symbol_id=None, direction=None, fromDate=None, wise="day"
):
    if direction == "up":
        direction = ">"
    elif direction == "down":
        direction = "<"
    else:
        direction = "<>"
    if fromDate is None:
        fromDate = datetime.datetime.min.date()
    if symbol_id is None:
        q = f"""
        SELECT streaks.*, assets.ticker, market.market_short_name
        FROM streaks
        JOIN assets ON streaks.symbol_id = assets.id
        JOIN market ON assets.market = market.market_id
        WHERE assets.ticker = '{symbol}'
        AND streaks.streak_gain {direction} 0.0 
        AND end_date >= '{fromDate}' 
        AND wise = '{wise}'
        ORDER BY streaks.end_date DESC
        ;"""
    else:
        q = f"""
        SELECT streaks.*, assets.ticker, market.market_short_name
        FROM streaks
        JOIN assets ON streaks.symbol_id = assets.id
        JOIN market ON assets.market = market.market_id
        WHERE streaks.symbol_id = {symbol_id}
        AND streaks.streak_gain {direction} 0.0 
        AND end_date >= '{fromDate}' 
        AND wise = '{wise}'
        ORDER BY streaks.end_date DESC
        ;"""
    streaksForSymbol = DfFromDb(q)
    return streaksForSymbol


def deleteAllFromRecentlyViewed(confirm=True):
    """
    Deletes all items from the "Recently Viewed" watchlist.

    Args:
        confirm (bool, optional): Prompt for confirmation before deletion. Defaults to True.

    Returns:
        None
    """

    if confirm:
        # Prompt for confirmation
        answer = input(
            "Are you sure you want to delete all items from the Recently Viewed watchlist? (y/N): "
        )
        if answer.lower() not in ("y", "yes"):
            print("Deletion cancelled.")
            return

    delete_query = """
        DELETE FROM watchlist_item
        WHERE watchlist_id IN (
            SELECT id FROM watchlist
            WHERE watchlist_name = 'Recently Viewed'
        );
        """

    try:
        query_database(delete_query, fetch=False)

    except Exception as e:
        print(f"Error deleting items: {e}")
