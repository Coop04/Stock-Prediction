#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=all
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W0718 # disbale too general exception warning
# pylint: disable=W1203 # disbale lazy formating in logging
# pylint: disable=R1705 # if else for better readability
# pylint: disable=R1711 # useless return, preserved for readability
# pylint: disable=W0105 # string statement has no effect

"""
Weekly update of streaks in the streaks_weekly table
"""

import sys
import multiprocessing

# import sys
# from math import pi
import time

# import pdb
# import datetime
# import yaml
import logging

# from bokeh.plotting import figure, show
# from bokeh.models import LinearAxis, DatetimeTickFormatter, WheelZoomTool

# import numpy as np
import pandas as pd

# from bokeh.models import HoverTool, ColumnDataSource
# import requests
if __name__ == "__main__":
    import db
    import from_api as fromApi
    import streaks as streaksLib
    import assets_mod as assetsMod
else:
    from . import db
    from . import from_api as fromApi
    from . import streaks as streaksLib
    from . import assets_mod as assetsMod
# import db
# import from_api as fromApi
# import streaks as streaksLib


logger = logging.getLogger(__name__)
logger.debug("Starting data_update.py")

processesParallel = 1


def DailyUpdateForAnAsset(asset):
    """The function which goes to multiprocessing pool to update daily_data"""

    symbol = asset["symbol"]
    symbol_id = asset["symbol_id"]

    try:
        streaksLib.UpdateStreakLatestForSymbol(symbol, symbol_id=symbol_id)
    except KeyboardInterrupt:
        logger.info("Daily update interrupted by user")
        sys.exit()
    except Exception as e:
        # failedSymbols.append(symbol)
        logger.warning(
            "%s streaksLib.UpdateStreakLaetstForSymbol failed for %s", e, symbol
        )

    # logger.debug("%s : df2Update: %s, streak: %s, Timetaken: %s", symbol, df2Update, streak, toc)


def Daily(exchange=None):
    """Update all running streaks"""

    wise = "day"  # not required but for making sql query uniform
    if exchange is None:
        logger.error("Market None for Daily")
        return
    logger.info("Starting data_update.Daily")
    # assets = AssetsToUpdate(exchange)
    assetsForDailyUpdate = AssetsForDailyUpdate(exchange)
    assetsForHistoricalUpdate = AssetsForHistoricalUpdate(exchange)
    assets = pd.concat([assetsForDailyUpdate, assetsForHistoricalUpdate], axis=0)

    with multiprocessing.Pool(processes=processesParallel) as pool:
        _results = pool.map(
            DailyUpdateForAnAsset, [asset for _, asset in assets.iterrows()]
        )
    logger.debug("results line done")
    pool.close()
    logger.debug("pool closed")
    pool.join()
    logger.debug("pool joined")

    streaksLib.RunningStreaksSummary2Db(exchange=exchange)
    logger.info("%s Running streaks summary sent to db", exchange)
    # print("failedSymbols:", failedSymbols)
    logger.info("%s Finished data_update.Daily", exchange)

    q = f"SELECT date FROM daily_data JOIN assets ON daily_data.id_asset = assets.id JOIN market ON assets.market = market.market_id WHERE market.market_short_name = '{exchange}' AND daily_data.wise = '{wise}' ORDER BY daily_data.date DESC LIMIT 1;"
    try:
        lastAggregatorUpdate = db.DfFromDb(q).iloc[0]["date"]
        # q = f"DELETE FROM last_aggregator_update JOIN market ON last_aggregator_update.market_id = market.market_id WHERE wise = '{wise}' AND market.market_short_name = '{exchange}';"
        q = f"""
            DELETE FROM last_aggregator_update
            USING market
            WHERE last_aggregator_update.market_id = market.market_id
            AND market.market_short_name = '{exchange}'
            AND last_aggregator_update.wise = '{wise}';
            """
        db.query_database(q, fetch=False)

        q = f"SELECT market_id FROM market WHERE market_short_name = '{exchange}';"
        market_id = db.DfFromDb(q).iloc[0]["market_id"]

        q = f"INSERT INTO last_aggregator_update (market_id, wise, date) VALUES ({market_id}, '{wise}', '{lastAggregatorUpdate}');"
        db.query_database(q, fetch=False)

        logger.info(
            "%s last_aggregator_market set to %s, in daily update",
            exchange,
            lastAggregatorUpdate,
        )
    except Exception as e:
        logger.error(
            f"{exchange}, LastAggregatorUpdate date failed, in daily update. {e}"
        )


def HistoryForAnAsset(asset, wise="day"):
    """The function which goes to multiprocessing pool to update history"""
    # tic = time.time()

    symbol = asset["symbol"]
    symbol_id = asset["symbol_id"]
    logger.debug(f"{symbol} Starting HisotryForAnAsset ")

    try:
        _ = streaksLib.UpdateStreaksAllForSymbol(symbol, symbol_id=symbol_id, wise=wise)
        # _ = streaksLib.UpdateStreakLatestForSymbol(
        #    symbol, symbol_id=symbol_id, wise=wise
        # )
    except KeyboardInterrupt:
        logger.info("History update interrupted by user")
        sys.exit()
    except Exception as e:
        # failedSymbols.append(symbol)
        logger.warning(
            "%s UpdateStreaksAllForSymbol: Failed for %s : %s", e, symbol, wise
        )

    logger.debug(f"{symbol} UpdateStreaks Done for HisotryForAnAsset ")
    # toc = time.time() - tic
    # logger.debug("%s : df2Update: %s, streaks: %s, Timetaken: %s", symbol, df2Update, streaks, toc)
    # logger.debug("%s : streaks : %s", symbol, streak)
    # logger.debug("%s : streak : %s", symbol, streak)
    # logger.debug("%s Timetaken : %s", symbol, toc)
    # print(50 * "-", symbol, 50 * "-")
    # print(50 * "-", symbol, 50 * "-")
    return None


def History(exchange=None, wise="day"):
    """Update all streaks, historical"""
    if exchange is None:
        logger.error("Market None for History")
        return
    logger.info("Starting data_update.Hisotry")
    assets = AssetsToUpdate(exchange)

    # pool = multiprocessing.Pool(processes=processesParallel)
    with multiprocessing.Pool(processes=processesParallel) as pool:
        _ = pool.starmap(
            HistoryForAnAsset, [(asset, wise) for _, asset in assets.iterrows()]
        )

    logger.debug("results line done")
    pool.close()
    logger.debug("pool closed")
    pool.join()
    logger.debug("pool joined")

    streaksLib.RunningStreaksSummary2Db(exchange=exchange, wise=wise)
    logger.info("%s %s Running streaks summary sent to db", exchange, wise)
    # print("failedSymbols:", failedSymbols)
    logger.info("%s %s Finished data_update.History", exchange, wise)

    q = f"SELECT date FROM daily_data JOIN assets ON daily_data.id_asset = assets.id JOIN market ON assets.market = market.market_id WHERE market.market_short_name = '{exchange}' AND daily_data.wise = '{wise}' ORDER BY daily_data.date DESC LIMIT 1;"
    try:
        lastAggregatorUpdate = db.DfFromDb(q).iloc[0]["date"]
        # q = f"DELETE FROM last_aggregator_update JOIN market ON last_aggregator_update.market_id = market.market_id WHERE wise = '{wise}' AND market.market_short_name = '{exchange}';"
        q = f"""
            DELETE FROM last_aggregator_update
            USING market
            WHERE last_aggregator_update.market_id = market.market_id
            AND market.market_short_name = '{exchange}'
            AND last_aggregator_update.wise = '{wise}';
            """
        db.query_database(q, fetch=False)

        q = f"SELECT market_id FROM market WHERE market_short_name = '{exchange}';"
        market_id = db.DfFromDb(q).iloc[0]["market_id"]

        q = f"INSERT INTO last_aggregator_update (market_id, wise, date) VALUES ({market_id}, '{wise}', '{lastAggregatorUpdate}');"
        db.query_database(q, fetch=False)

        # q = f"UPDATE market SET last_aggregator_update = '{lastAggregatorUpdate}' WHERE market_short_name = '{exchange}';"
        logger.info(
            "%s last_aggregator_market set to %s, in history update",
            exchange,
            lastAggregatorUpdate,
        )
    except Exception as e:
        logger.error(
            f"{exchange}, LastAggregatorUpdate date failed, in history update. {e}"
        )


def AssetsToUpdate(exchange):
    """Creates the assets dataframe to update"""
    q = f"SELECT assets.id as symbol_id, assets.ticker as symbol FROM assets JOIN market ON assets.market = market.market_id WHERE market.market_short_name = '{exchange}' AND assets.asset_type = 1 AND assets.status = 'Active';"
    assets1 = db.DfFromDb(q)
    q = f"SELECT assets.id as symbol_id, assets.ticker as symbol FROM assets JOIN market ON assets.market = market.market_id WHERE market.market_short_name = '{exchange}' AND assets.asset_type = 49 AND assets.status = 'Active';"
    assets49 = db.DfFromDb(q)
    # assets = pd.concat([assets1, assets49])

    q = f"SELECT assets.id as symbol_id, assets.ticker as symbol FROM assets JOIN market ON assets.market = market.market_id WHERE market.market_short_name = '{exchange}' AND assets.asset_type = 60 AND assets.status = 'Active';"
    assets60 = db.DfFromDb(q)
    assets = pd.concat([assets1, assets49, assets60])
    return assets


def AssetsForHistoricalUpdate(exchange):
    """Assets for which historical update needs to be done due to price adjustment. Adjustment estimated from FMP API"""
    assetsInExchange = AssetsToUpdate(exchange)
    openDaysExchange = fromApi.OpenDaysExchange(exchange, limit=1)
    dfBulk = fromApi.BulkDayFmg(openDaysExchange[0])
    dfBulkInExchange = pd.merge(dfBulk, assetsInExchange, on="symbol", how="inner")
    assetsForHistoricalUpdate = dfBulkInExchange[
        dfBulkInExchange["adjClose"] != dfBulkInExchange["close"]
    ]
    return assetsForHistoricalUpdate


def AssetsForDailyUpdate(exchange):
    """Assets which requires daily update. If the last 3 days trend is monotonous, then select for daily update"""
    assetsInExchange = AssetsToUpdate(exchange)
    openDaysExchange = fromApi.OpenDaysExchange(exchange, limit=4)
    dfBulk0 = fromApi.BulkDayFmg(openDaysExchange[0])
    dfBulk0 = dfBulk0[["symbol", "adjClose"]]
    dfBulk0 = pd.merge(
        dfBulk0, assetsInExchange, on="symbol", how="inner"
    )  # only symbols in exchange retained, rest filtered out

    time.sleep(5)
    dfBulk1 = fromApi.BulkDayFmg(openDaysExchange[1])
    dfBulk1 = dfBulk1[["symbol", "adjClose"]]
    dfBulk1 = pd.merge(dfBulk1, dfBulk0, on="symbol", how="inner", suffixes=("1", "0"))

    time.sleep(5)
    dfBulk2 = fromApi.BulkDayFmg(openDaysExchange[2])
    dfBulk2 = dfBulk2[["symbol", "adjClose"]]
    dfBulk2 = dfBulk2.rename(columns={"adjClose": "adjClose2"})
    dfBulk2 = pd.merge(dfBulk2, dfBulk1, on="symbol", how="inner")

    time.sleep(5)
    dfBulk3 = fromApi.BulkDayFmg(openDaysExchange[3])
    dfBulk3 = dfBulk3[["symbol", "adjClose"]]
    dfBulk3 = dfBulk3.rename(columns={"adjClose": "adjClose3"})
    dfBulk = pd.merge(dfBulk3, dfBulk2, on="symbol", how="inner")

    dfBulkDiff = (
        dfBulk[["adjClose3", "adjClose2", "adjClose1", "adjClose0"]]
        .diff(axis=1)
        .drop(["adjClose3"], axis=1)
    )
    dfBulkUpLogical = dfBulkDiff >= 0.0
    assetsWithUpStreak = dfBulk[dfBulkUpLogical.all(axis=1)]

    dfBulkDownLogical = dfBulkDiff <= 0.0
    assetsWithDownStreak = dfBulk[dfBulkDownLogical.all(axis=1)]

    assetsForDailyUpdate = pd.concat([assetsWithUpStreak, assetsWithDownStreak], axis=0)
    assetsForDailyUpdate = pd.concat([assetsWithUpStreak, assetsWithDownStreak], axis=0)
    return assetsForDailyUpdate


def AssetsUpdate():
    """
    Update All Assets with input from FMG API
    """
    try:
        assetsMod.AssetsToDb()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt from AssetsUpdate data_upadte.py")
        sys.exit()
    except Exception as e:
        logger.error(e)
    logger.info("AssetsUpdate from data_update finished")


def Actives():
    """Update Active Dormant status of all assets"""
    try:
        assetsMod.Actives()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt from Actives data_upadte.py")
        sys.exit()
    except Exception as e:
        logger.error(e)
    logger.info("Actives from data_update finished")


"""
if __name__ == "__main__":
    assets = db.ReadTable("assets")
    assets = assets.loc [assets["asset_type"] == 1]
    tickers = assets["ticker"]
    lenTickers = len(tickers)
    k = 0
    while True:
    # while k < 2000:
    # for ticker in tickers:

        tic = time.time() 
        ticker = tickers.iloc[k]
        try:
            df, dfFromDb, df2Update = UpdateSymbol(ticker)
        except Exception as e:
            logger.warning("%s UpdateSymbol from data_udate.py Failed for %s", e, ticker)
        try:
            streak = UpdateStreakLatestForSymbol(symbol=ticker, symbol_id=symbol_id)
            print(50 * "-", symbol, 50 * "-")
            print(streak)
        except Exception as e:
            logger.warning("%s UpdateRunningStreaksAll: Failed for %s", e, ticker)
        # print(k, "/", len(tickers), ticker)
        k = k+1
        toc = time.time() - tic
        toFinish = datetime.datetime.now() + datetime.timedelta(seconds=(lenTickers-k) * toc ) 
        print ("finishing at:", toFinish, "     toc=", toc, "   track:", k, "/", lenTickers, ticker)
"""
