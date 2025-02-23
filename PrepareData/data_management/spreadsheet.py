# pylint: disable=E0401
# pylint: disable=C0103

"""This module is methods for spreadsheet data requirements"""
import pandas as pd

import os
import yaml
import psycopg2
import pandas as pd
import datetime

if __name__ == "__main__":
    # import db
    import from_api as fromApi
else:
    # from . import db
    from . import from_api as fromApi


# @profile
def Ma(symbol, windowSize):
    """Moving Average"""
    # df = db.ReadTableDailyData(symbol, limit=windowSize, idAsset=None)
    df = fromApi.DailyDfFromFmg(symbol, limit=windowSize)
    ma = df["close"].mean()
    return ma

# profile
def MaFromApi(symbol, windowSize):
    """Moving Average"""
    # df = db.ReadTableDailyData(symbol, limit=windowSize, idAsset=None)
    windowSize = datetime.datetime.today().date() - datetime.timedelta(days=windowSize)
    df = fromApi.DailyDfFromFmg(symbol, windowSize)
    ma = df["close"].mean()

def Gain(symbol, period):
    """Gain for a period"""
    # df = db.ReadTableDailyData(symbol, limit=period, idAsset=None)
    df = fromApi.DailyDfFromFmg(symbol, limit=period)
    gain = (df["close"].iloc[0] - df["close"].iloc[-1]) / df["close"].iloc[-1]
    gain = gain * 100
    gain = round(gain, 2)
    return gain

# @profile
def MaClassic(symbol, windowSize):
    current_directory = os.getcwd()

    def load_config(filename):
        """
        Method to load config file 
        """
        with open(filename, 'r', encoding='utf-8') as file:
            configs = yaml.safe_load(file)
        return configs

    # Load database configuration from config.yml
    config = load_config('../config.yml')

    # Connect to PostgreSQL using configuration from config.yml
    conn = psycopg2.connect(
        dbname=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password'],
        host=config['database']['host']
    )

    # Create cursor
    cursor = conn.cursor()

    q = f"""
        SELECT dd.date, dd.close, dd.volume as vol, dd.open, dd.high, dd.low
        FROM daily_data dd
        JOIN assets a ON dd.id_asset = a.id
        WHERE a.ticker = '{symbol}'
        ORDER BY dd.id DESC
        LIMIT {windowSize};
        """
    df = pd.read_sql(q, conn)
    # df = db.ReadTableDailyData(symbol, limit=windowSize, idAsset=None)
    ma = df["close"].mean()
    # Commit changes
    # conn.commit()

    # Close cursor and connection
    cursor.close()
    conn.close()
    return ma

# @profile
def MaUri(symbol, windowSize):
    current_directory = os.getcwd()

    def load_config(filename):
        """
        Method to load config file 
        """
        with open(filename, 'r', encoding='utf-8') as file:
            configs = yaml.safe_load(file)
        return configs

    # Load database configuration from config.yml
    config = load_config('../config.yml')
    user = config['database']['user']
    password = config['database']['password']
    host = config['database']['host']
    dbname = config['database']['dbname']

    db_uri = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{dbname}"

    q = f"""
        SELECT dd.date, dd.close, dd.volume as vol, dd.open, dd.high, dd.low
        FROM daily_data dd
        JOIN assets a ON dd.id_asset = a.id
        WHERE a.ticker = '{symbol}'
        ORDER BY dd.id DESC
        LIMIT {windowSize};
        """
    df = pd.read_sql(q, db_uri)
    # df = db.ReadTableDailyData(symbol, limit=windowSize, idAsset=None)
    ma = df["close"].mean()
    return ma

if __name__ == "__main__":
    limit = 1
    print(Ma("AAPL", limit))
    print(MaFromApi("AAPL", limit))
    # print(MaClassic("AAPL", limit))
    # print(MaUri("AAPL", limit))

def Price(symbol, day=1, ochl="close"):
    df = fromApi.DailyDfFromFmg(symbol, limit=day)
    if not isinstance(df, pd.DataFrame):
        return None
    if len(df) < 1:
        return None
    price = df.iloc[-1][ochl]
    return price


def Volume(symbol, period=5):
    df = fromApi.DailyDfFromFmg(symbol, limit=period)
    if not isinstance(df, pd.DataFrame):
        return None
    if len(df) < 1:
        return None
    volume = df["volume"].sum()
    return volume


"""
AppScript to be used in the googlesehet

function fetchURL(url) {
  var options = {
    'muteHttpExceptions': true,
    'timeout': 600000 // timeout in milliseconds (e.g., 60000 ms = 60 seconds)
  };

  try {
    var response = UrlFetchApp.fetch(url, options);
    var responseCode = response.getResponseCode();

    if (responseCode === 200) {
      return response.getContentText();
    } else if (responseCode === 404) {
      return "No Symbol";
    } else {
      return "Unexpected Error: " + responseCode;
    }
  } catch (e) {
    return "No Symbol";
  }
}
"""

