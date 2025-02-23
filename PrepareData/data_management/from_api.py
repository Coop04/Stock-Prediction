"""
This module takes care of interacting with alphavantage data api
"""

# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W0718 # disable snake_case warning
# pylint: disable=W1203 # disable snake_case warning
# pylint: disable=W0105 # disable snake_case warning

import logging
import urllib
import time
from datetime import datetime, timedelta
import yaml
import requests
import pandas as pd

if __name__ in ["__main__", "from_api"]:
    import weekly_monthly as wm
else:
    from . import weekly_monthly as wm

logger = logging.getLogger(__name__)
logger.debug("Starting streaks.py")


def load_config(filename):
    """To load config file"""
    with open(filename, "r", encoding="utf-8") as file:
        configs = yaml.safe_load(file)
    return configs


config = load_config("../config.yml")
apikey = config["alphavantage"]["apikey"]
urlBase = "https://www.alphavantage.co/query?"
apikeyFmg = config["fmg"]["apikey"]


urlBaseFmg4 = "https://financialmodelingprep.com/api/v4/"
urlBaseFmg3 = "https://financialmodelingprep.com/api/v3/"

urlBulkDay = "batch-request-end-of-day-prices?date={date}&apikey={apikey}"

urlAssetsFmg = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={config["fmg"]["apikey"]}'
urlIndexesFmg = f'https://financialmodelingprep.com/api/v3/symbol/available-indexes?apikey={config["fmg"]["apikey"]}'
urlCompanyOverview = "https://financialmodelingprep.com/api/v3/profile/"  # AAPL?apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9
urlIsTheMarketOpen = "https://financialmodelingprep.com/api/v3/is-the-market-open?"

urlCryptoAssetsAll = f"https://financialmodelingprep.com/api/v3/symbol/available-cryptocurrencies?apikey={apikeyFmg}"

"https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices?date=2021-05-18?apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9"
# urlIsTheMarketOpen = "https://financialmodelingprep.com/api/v3/is-the-market-open?exchange=NSE&apikey={config['fmg']['apikey']}"

# week/month/day wise data, but missing adjusted close
# https://financialmodelingprep.com/api/v3/historical-chart/1day/AAPL?apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9
# urlHistory = urlBaseFmg3 +  "historical-chart/" + "{interval}/{symbol}?apikey={apikey}"

# https://financialmodelingprep.com/api/v3/technical_indicator/1day/AAPL?type=rsi&period=14&apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9
# urlRsi = "https://financialmodelingprep.com/api/v3/technical_indicator/"  # 1day/AAPL?type=rsi&period=14&apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9

exchangeOpenCheckCompanies = {
    "NASDAQ": "AAPL",
    "NYSE": "BRK-A",
    "SAU": "2222.SR",
    "DFM": "EMAAR.AE",
    "NSE": "RELIANCE.NS",
    "CRYPTO": "BTCUSD",
}


def BulkDayFmg(date):
    """Returns close day / instance price for all commodities in all markets covered by FMP"""
    date = str(date)
    url = urlBaseFmg4 + urlBulkDay.format(date=date, apikey=apikeyFmg)
    while True:
        try:
            df = pd.read_csv(url)
            logger.info(f"Bulk Data Read for {date}")
            if len(df) > 0:
                return df #### DANGER
        except urllib.error.HTTPError as e:
            logger.warning(e)
            if e.code == 429:
                pass
            else:
                return None
        except Exception as e:
            logger.warning(e)
            return None


def OpenDaysExchange(exchange, limit=4):
    """
    Returns the last limit number of days the exchange is open
    This  method is used for filtering out exchanges without significant streak related changes.
    """
    symbol = exchangeOpenCheckCompanies[exchange]
    df = DailyDfFromFmg(symbol, limit=limit)
    openDaysExchange = list(df["date"].values)
    return openDaysExchange


def DailyDfFromFmg(symbol, fromDate="1900-01-01", limit=None, wise="day", latestTail=False):
    """Method to fetch historical EOD adjusted close data from FMP"""
    limitDays = limit
    if limitDays is not None:
        limitDays = int(limitDays)
        limitDaysToFetch = limitDays + 10
        fromDate = datetime.today().date() - timedelta(limitDaysToFetch)
    urlHistorical = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={apikeyFmg}&&from={str(fromDate)}"
    callApi = True
    while callApi:
        r = requests.get(urlHistorical, timeout=60)
        if r.status_code == 200:
            callApi = False
            d = r.json()
            if "historical" in d.keys():
                df = pd.DataFrame(d["historical"])
                df = df[["date", "open", "high", "low", "close", "adjClose", "volume"]]
                multiplicationRatioForAdjustment = df["adjClose"] / df["close"]
                df["open"] = df["open"] * multiplicationRatioForAdjustment
                df["high"] = df["high"] * multiplicationRatioForAdjustment
                df["low"] = df["low"] * multiplicationRatioForAdjustment
                df["volume"] = df["volume"] * multiplicationRatioForAdjustment
                df["close"] = df["adjClose"]
                df.pop("adjClose")
                df["volume"] = df["volume"] / 1000000
                df["date"] = pd.to_datetime(df["date"]).dt.date
                if limitDays is not None:
                    df = df.iloc[0:limitDays]
                # df.index = df["timestamp"]
                # df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
                df = wm.DfResample(df, wise, latestTail)
                df["date"] = df["date"].dt.date
                df.reset_index(inplace=True)
                return df
            # else:
            # return None
        else:
            callApi = True
            print("Excceded API limit, waiting 10 s")
            time.sleep(10)
    return None


def CompanyOverview(symbol):
    """Method to get overview of the company"""
    urlQuery = f"{urlCompanyOverview}{symbol}?apikey={config['fmg']['apikey']}"
    companyOverview = pd.read_json(urlQuery)
    if companyOverview is None:
        return None
    if len(companyOverview) > 0:
        companyOverview = companyOverview.iloc[0]
        return companyOverview
    return None


def Assets():
    """Method to get the list of symbols required from FMG API"""
    assets = pd.read_json(urlAssetsFmg)
    assets = assets[
        assets["exchangeShortName"].isin(["NASDAQ", "NYSE", "DFM", "SAU", "NSE"])
    ]
    # assets = assets[assets["type"] == "stock"]
    assets = assets[(assets["type"] == "stock") | (assets["type"] == "etf")]
    return assets


def CryptoAssetsAll():
    """Read crypto assets all from FMP"""
    dfCryptoAssetsAll = pd.read_json(urlCryptoAssetsAll)
    dfCryptoAssetsAll.rename(columns={"symbol": "ticker"}, inplace=True)
    return dfCryptoAssetsAll


'''
Looks like this method is not used anywhere in the code.
Also a test found that the API is returning wrong result 
def IsTheMarketOpen(exchange=None):
    """Method to check if the market is open"""
    if not isinstance(exchange, str):
        return None
    urlMarketOpen = (
        urlIsTheMarketOpen + f"exchange={exchange}&apikey={config['fmg']['apikey']}"
    )
    r = requests.get(urlMarketOpen)
    if r.status_code != 200:
        logger.error(f"IsTheMarketOpen API failed for {exchange}")
        return None
    d = r.json()
    isTheMarketOpen = d["isTheStockMarketOpen"]
    return isTheMarketOpen
'''

'''
This method is not used. Hence commenting out for the time being. 
To be deleted later
def Indexes():
    """Work In Progress, read all indexes, available"""
    indexes = pd.read_json(urlIndexesFmg)
    return indexes
    """ indexes = indexes[indexes["exchangeShortName"].isin(["NASDAQ", "DFM", "SAU", "NSE"])]
    return indexes
    indexes = indexes[indexes["type"]=="stock"]
    return indexes  """
'''


def Rsi(symbol, interval="1day", period=14):
    """Method to return RSI metric for the spreadsheet APIs"""
    period = str(period)
    urlRsi = f"https://financialmodelingprep.com/api/v3/technical_indicator/{interval}/{symbol}?type=rsi&period={period}&apikey={config['fmg']['apikey']}"
    try:
        df = pd.read_json(urlRsi)
        rsi = df.iloc[0]["rsi"]
    except Exception as e:
        logger.error(e)
        rsi = ""
    return rsi
