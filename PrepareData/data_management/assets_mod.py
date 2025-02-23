"""
This module takes care of managing assets list
"""

# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W1203 # disable lazy % formatting warning, as this formatting offers better readability
# pylint: disable=R0914 # disable too many local variables
# pylint: disable=R1705 # I prefer if else explicit for clarity
# pylint: disable=W0718 # general exception Exception

# from datetime import datetime, timedelta
import logging
import re
import sys
import pandas as pd
from logger_ import logger

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
logger.debug("Starting streaks.py in debug level")
print(logging.getLevelName(logger.getEffectiveLevel()))


# import yaml
# import requests

if __name__ in ["__main__", "assets_mod"]:
    import db
    import from_api as fromApi
else:
    from . import db
    from . import from_api as fromApi


def Active(assetFromDb):
    """Checks if an asset in the db is actively trading or not, marks Dormant in the assets table if not active"
    Args:
        assetFromDb, a row from the assets db
    Returns:
        True if active, False if found not actively trading, and marks dormant
    """
    symbol = assetFromDb["ticker"]
    # print("symbol", symbol)
    companyOverview = fromApi.CompanyOverview(symbol)
    status = True
    if companyOverview is None:
        status = False
    elif not companyOverview["isActivelyTrading"]:
        status = False
    else:
        status = True
    idAsset = assetFromDb["id"]
    if status is False:
        q = f"UPDATE assets SET status = 'Dormant' WHERE id = {idAsset};"
        db.query_database(q, fetch=False)
        logger.info("%s marked Dormant", symbol)
    else:
        q = f"UPDATE assets SET status = 'Active' WHERE id = {idAsset};"
        db.query_database(q, fetch=False)
        logger.info("%s marked Active", symbol)
    return status


def Actives():
    """Reads DB and executes Active()"""
    assetsFromDb = db.ReadTable("assets")
    activeNots = []
    k = 0
    for _, assetFromDb in assetsFromDb.iterrows():
        active = Active(assetFromDb)
        if not active:
            activeNots.append(assetFromDb)
            k = k + 1
            # print(assetFromDb)
        else:
            # print(k, "/", len(assetsFromDb))
            k = k + 1
        logger.debug(f"{k} / {len(assetsFromDb)} : Actives")
    return activeNots


# def MarektsDbUpdate(assetFromApi):


def SymbolInDb(symbol):
    """Checking if symbol is in DB assets table"""
    symbolInDb = False
    querySymbolInDb = f"SELECT * FROM assets WHERE ticker = '{symbol}';"
    assetFromDb = db.DfFromDb(querySymbolInDb)
    if len(assetFromDb) == 1:
        logger.info("%s in assets", symbol)
        assetFromDb = assetFromDb.iloc[0]
        symbolInDb = True
    elif len(assetFromDb) > 1:
        logger.error("%s has multiple entries in assets table", symbol)
        symbolInDb = False
    else:
        logger.info("%s NOT in assets", symbol)
        symbolInDb = False
    return symbolInDb, assetFromDb


def AssetToDb(assetFromApi):
    """
    Update or Insert asset Information from FMG API to assets table in db
    Args:
        assetFromApi: pd.Dataseries
    Returns:
        None
    """
    symbol = assetFromApi["symbol"]
    symbolInDb, assetFromDb = SymbolInDb(symbol)
    name = assetFromApi["name"]
    name = re.sub(r"[']", "", name)
    marketShortName = assetFromApi["exchangeShortName"]
    queryMarket = (
        f"SELECT market_id FROM market WHERE market_short_name = '{marketShortName}';"
    )
    market_id = db.DfFromDb(queryMarket)

    if len(market_id) == 1:
        market_id = market_id.iloc[0]["market_id"]
    else:
        logger.error("%s not in markets table, for asset %s", marketShortName, symbol)
        return None
    typeOfAsset = assetFromApi["type"]
    if typeOfAsset != "stock":
        logger.error("%s not in asset_type table for %s", typeOfAsset, symbol)
        return None
    companyOverview = fromApi.CompanyOverview(symbol)
    sector = companyOverview["sector"]
    logoUrl = companyOverview["image"]
    marketCap = companyOverview["mktCap"]
    logger.debug(f"{symbol} logoUrl: {logoUrl}")
    if companyOverview["isActivelyTrading"]:
        status = "Active"
    else:
        status = "Dormant"
        logger.info("%s is Dormant")

    if symbolInDb:
        symbol_id = assetFromDb["id"]

        qUpdate = f"""
                    UPDATE assets
                    SET name = '{name}',
                        market = {market_id},
                        sector = '{sector}',
                        logo_url = '{logoUrl}',
                        market_cap = {marketCap}
                    WHERE id = {symbol_id};
                    """
        r = db.query_database(qUpdate, fetch=False)
        return r
    else:
        qInsert = f"""
            INSERT INTO assets (ticker, name, market, asset_type, status, sector, logo_url)
            VALUES ('{symbol}', '{name}', {market_id}, 1, '{status}', '{sector}', '{logoUrl}');
            """
        r = db.query_database(qInsert, fetch=False)
        logger.info("%s inserted to assets table")
        return r


def AssetsToDb():
    """
    Read Assets from API and add to assets db upon required.
    """
    logger.debug("Starting AssetsToDb")
    assetsFromApi = fromApi.Assets()
    lenAssetsFromApi = len(assetsFromApi)
    for k in range(lenAssetsFromApi):
        assetFromApi = assetsFromApi.iloc[k]
        try:
            _r = AssetToDb(assetFromApi)
            logger.debug(
                "%s / %s AssetsToDb from assets_mod.py : %s",
                k,
                lenAssetsFromApi,
                assetFromApi,
            )
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupted AssetsToDb from assets_mod.py")
            sys.exit()
        except Exception as e:
            logger.error("%s AssetstoDb from assets_mod.py", e)

def _insert_non_duplicates(df, table_name, engine, unique_column='ticker'):
    # Get existing tickers from the database
    query = f"SELECT {unique_column} FROM {table_name}"
    existing_tickers = pd.read_sql_query(query, engine)[unique_column].tolist()
    
    # Filter out rows that already exist in the database
    new_records = df[~df[unique_column].isin(existing_tickers)]
    
    # Insert only if there are new records
    if not new_records.empty:
        new_records.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Inserted {len(new_records)} new records")
    else:
        print("No new records to insert")
    
    return new_records

def IndicesInsert():
    """Method to insert indices in the assets table, hard coded"""
    indices = ["^DJI", "^IXIC", "^NSEI", "^TASI.SR", "^GSPC"]
    names = [
        "Dow Jones Industrial Average",
        "NASDAQ Composite",
        "NIFTY 50",
        "Tadawul All Shares Index",
        "Standard and Poors 500"
    ]
    marketNames = ["NASDAQ", "NASDAQ", "NSE", "SAU", "NYSE"]

    dfIndices = pd.DataFrame({"ticker": indices, "name": names, "market": marketNames})
    q = "SELECT market_short_name, market_id FROM market;"
    mappingDf = db.DfFromDb(q)
    exchangeToId = dict(zip(mappingDf["market_short_name"], mappingDf["market_id"]))
    dfIndices["market"] = dfIndices["market"].map(exchangeToId)

    q = "SELECT id FROM asset_type WHERE asset_type = 'Index';"
    dfIndices["asset_type"] = db.DfFromDb(q).iloc[0]["id"]

    dfIndices["status"] = "Active"
    dfIndices["sector"] = "Market Index"

    new_records = _insert_non_duplicates(dfIndices, "assets", db.engine)

    # dfIndices.to_sql("assets", db.engine, if_exists="append", index=False)

    print("Indices added to assets")

    return dfIndices


def CryptosInsert():
    """Method to insert crypto assets to assets, hard coded"""
    cryptos = [
        "BTCUSD",
        "ETHUSD",
        "USDTUSD",
        "BNBUSD",
        "SOLUSD",
        "USDCUSD",
        "XRPUSD",
        "DOGEUSD",
        "TRXUSD",
        "TONUSD",
        "ADAUSD",
        "AVAXUSD",
        "SHIBUSD",
        "BCHUSD",
        "LINKUSD",
        "DOTUSD",
        "NEARUSD",
        "LEOUSD",
        "SUIUSD",
        "DAIUSD",
    ]

    dfCryptoAssetsAll = fromApi.CryptoAssetsAll()
    dfCryptoAssets = dfCryptoAssetsAll[dfCryptoAssetsAll["ticker"].isin(cryptos)]
    dfCryptoAssets = dfCryptoAssets[["ticker", "name"]]

    qReadAssetTypeCrypto = (
        "SELECT asset_type FROM asset_type WHERE asset_type = 'Crypto';"
    )
    assetTypeCrypto = db.DfFromDb(qReadAssetTypeCrypto)
    if len(assetTypeCrypto) == 0:
        assetTypeCrypto = dict({"asset_type": ["Crypto"]})
        assetTypeCrypto = pd.DataFrame(assetTypeCrypto)
        assetTypeCrypto.to_sql("asset_type", db.engine, if_exists="append", index=False)
        print("Crypto asset_type added to asset_type table")

    qReadMarketCrypto = (
        "SELECT market_short_name FROM market WHERE market_short_name = 'CRYPTO';"
    )
    marketCrypto = db.DfFromDb(qReadMarketCrypto)
    if len(marketCrypto) == 0:
        marketCrypto = dict(
            {
                "market_short_name": ["CRYPTO"],
                "market_long_name": ["Crypto Currency"],
                "currency": ["USD"],
                "country": ["USA"],
            }
        )
        marketCrypto = pd.DataFrame(marketCrypto)
        marketCrypto.to_sql("market", db.engine, if_exists="append", index=False)
        print("Crypto added to market table")

    qMarketCrypto = "SELECT market_id FROM market WHERE market_short_name = 'CRYPTO';"
    idMarketCrypto = db.DfFromDb(qMarketCrypto).iloc[0]["market_id"]
    dfCryptoAssets["market"] = idMarketCrypto

    qAssetCrypto = "SELECT id FROM asset_type WHERE asset_type = 'Crypto';"
    idAssetCrypto = db.DfFromDb(qAssetCrypto).iloc[0]["id"]
    dfCryptoAssets["asset_type"] = idAssetCrypto

    dfCryptoAssets["sector"] = "Crypto"
    dfCryptoAssets["status"] = "Active"

    dfCryptoAssets.to_sql("assets", db.engine, if_exists="append", index=False)

    return dfCryptoAssets


if __name__ == "__main__":
    # x = CryptosInsert()
    # print(x)
    pass 
