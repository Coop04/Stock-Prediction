#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module takes care of calculating and playing with streaks

"""
# pylint: disable=all
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W0718 # disable blanket exception
# pylint: disable=W1203 # disbale lazy formating in logging
# pylint: disable=R0911 # disbale too many returns
import datetime
import pandas as pd
from collections import Counter
import numpy as np
import logging
import time

if __name__ in ["__main__", "streaks"]:
    import db
    import from_api as fromApi
else:
    from . import db
    from . import from_api as fromApi

logger = logging.getLogger(__name__)
logger.debug("Starting streaks.py")


def StreakLatestFromDf(df, streakLenMin=3, call=1):
    """
    Calculates the streak from df. If df length is small and streak is bigger than the df, then a fetch complete history flag is returned.
    """
    streak = None
    fetchWholeFlag = False
    columnNames = ["start_date", "end_date", "streak_len", "streak_gain", "streak_vol"]
    streak = pd.DataFrame(columns=columnNames)
    if not isinstance(df, pd.DataFrame):
        # if type(df) != pd.DataFrame:
        return streak, fetchWholeFlag
    df = df.copy()
    df["diff"] = df["close"].diff(periods=-1)
    df["up"] = df["diff"] > 0.0
    df["down"] = df["diff"] < 0.0
    streakLens = df[["up", "down"]].cumprod().sum()
    streakLen = streakLens.max()
    limit = len(df) - 1
    if (call == 1) and (streakLen >= (limit - 1)):
        fetchWholeFlag = True
    elif streakLen >= streakLenMin:
        startDate = df.iloc[streakLen - 1]["date"]
        endDate = df.iloc[0]["date"]
        df["gainPerc"] = 100 * df["diff"].iloc[0:-1] / df["close"].iloc[1:].values
        streakGain = df["gainPerc"].iloc[0:streakLen].sum()
        streakVol = df["volume"].iloc[0:streakLen].sum()
        streakDfClose = df.iloc[0:streakLen]["close"].values.tolist()
        streak = {
            "start_date": [startDate],
            "end_date": [endDate],
            "streak_len": [streakLen],
            "streak_gain": [streakGain],
            "streak_vol": [streakVol],
            "streak_df_close": [streakDfClose],
        }
        streak = pd.DataFrame(streak)
        # streak["symbol"] = symbol
    # elif streakLen <= 2:
    return streak, fetchWholeFlag


def UpdateStreakLatestForSymbol(symbol, symbol_id=None, wise="day"):
    """
    Given symbol or symbol_id fetches df from API and calculated the latest streak
    Update latest streak to db table named streaks
    """

    if symbol_id is None:
        symbol_id = db.IdAsset(symbol)

    # df = fromApi.DailyDfFromAlphaVantage(symbol, outputsize="compact")
    closeLast = db.CloseLast(symbol, idAsset=symbol_id, wise=wise)
    if len(closeLast) == 0:
        UpdateStreaksAllForSymbol(symbol, symbol_id=symbol_id, wise=wise)
        return

    if len(closeLast) > 1:
        logger.error(
            f"There are mulitple rows in daily_data for {symbol}, which is not permitted"
        )
    fromDate = closeLast.iloc[0]["date"]
    fromDateClose = closeLast.iloc[0]["close"]

    df = fromApi.DailyDfFromFmg(symbol, fromDate=fromDate, limit=100, wise=wise)
    if fromDate not in df["date"].values:
        UpdateStreaksAllForSymbol(symbol, symbol_id=symbol_id, wise=wise)
        return

    # check if there was an adjustment after the last fetch and streak workout
    if df[df["date"] == fromDate]["close"].iloc[0] != fromDateClose:
        UpdateStreaksAllForSymbol(symbol, symbol_id=symbol_id)
        return

    if df.iloc[0]["date"] == fromDate:
        logger.info("fromDate is same as latest date returned from API")
        return

    # df = db.ReadTableDailyData(symbol, limit=100, idAsset=symbol_id)
    streak, fetchWholeFlag = StreakLatestFromDf(df, streakLenMin=3, call=1)

    if len(streak) == 1:
        if symbol_id is None:
            q = f"SELECT id FROM assets WHERE ticker = '{symbol}';"
            symbol_id = db.DfFromDb(q)
            if len(symbol_id) == 1:
                symbol_id = symbol_id.iloc[0]["id"]
            else:
                logger.warning(f"{symbol} not in assets db or multiple symbol entries")
                return None
        streak["symbol_id"] = symbol_id
        streak["wise"] = wise
        db.Streak2StreaksTable(streak, wise=wise)

    elif fetchWholeFlag is True:
        df = fromApi.DailyDfFromFmg(symbol, wise=wise)
        # df = fromApi.DailyDfFromAlphaVantage(symbol, outputsize="full")
        # df = db.ReadTableDailyData(symbol, limit=None, idAsset=symbol_id)
        streak, fetchWholeFlag = StreakLatestFromDf(df, streakLenMin=3, call=2)
        if len(streak) == 1:
            if symbol_id is None:
                q = f"SELECT id FROM assets WHERE ticker = '{symbol}';"
                symbol_id = db.DfFromDb(q)
                if len(symbol_id) == 1:
                    symbol_id = symbol_id.iloc[0]["id"]
                else:
                    logger.warning(
                        f"{symbol} is not in assets db or multiple symbol entries"
                    )
                    return None
            streak["symbol_id"] = symbol_id
            streak["wise"] = wise
            db.Streak2StreaksTable(streak, wise=wise)

    ds = df.iloc[0]
    # db.CloseLastSet(symbol, ds, idAsset=symbol_id, wise=wise) #This method commented out in db
    db.CloseLastInsert(symbol, ds, idAsset=symbol_id, wise=wise)
    return streak


def HistStreaksRunning(direction="", exchange=None, wise="day"):
    """
    Histogram of the running streaks, for the barchart
    """
    if exchange is None:
        logger.error("HistStreaksRunning exchange is None")
        return
    # if (direction == "up") or (direction == "down"):
    # if direction in ["up", "down"]:
    if direction == "down":
        compare = "<"
    elif direction == "up":
        compare = ">"
    else:
        return None

    qStreaksLens = f"SELECT srs.streak_len FROM streaks_running_summary srs \
        JOIN assets ON srs.symbol_id = assets.id \
        JOIN market on assets.market = market.market_id \
        WHERE gain_running {compare} 0 \
        AND market.market_short_name = '{exchange}' \
        AND wise = '{wise}' \
        ;"

    streaksLens = db.DfFromDb(qStreaksLens)
    # streaksRunning = db.ReadRunningStreaks(direction)
    histRunningStreaks = dict(Counter(streaksLens["streak_len"]))
    return histRunningStreaks


def StreaksAllFromDf(df, streakLenMin=3, direction=""):
    """Right method to calculate all streaks"""

    if direction not in ["up", "down"]:
        raise ("direction parameter should be either up/down and must be specified")

    streaksAll = pd.DataFrame(
        columns=["start_date", "end_date", "streak_len", "streak_gain", "streak_vol"]
    )
    if df is None:
        return streaksAll, df
    if len(df) == 0:
        return streaksAll, df
    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    df["diff"] = df["close"].diff(periods=-1)
    df["gainPerc"] = 100 * df["diff"].iloc[0:-1] / df["close"].iloc[1:].values
    df["up"] = df["diff"] >= 0.0
    df["down"] = df["diff"] <= 0.0

    def g(df):
        groups = []
        curr_group = []
        for idx, row in df.iterrows():
            if row[direction]:
                curr_group.append(idx)
            elif curr_group:
                groups.append(curr_group.copy())
                curr_group = []
        if curr_group:
            groups.append(curr_group)
        return groups

    groups = g(df.copy())
    for group in groups:
        if len(group) >= streakLenMin:
            dfStreak = df.iloc[group]
            start_date = dfStreak.iloc[-1]["date"]
            end_date = dfStreak.iloc[0]["date"]
            streak_len = len(dfStreak)
            streak_gain = dfStreak["gainPerc"].sum()
            streak_vol = dfStreak["volume"].sum()
            streaksAll.loc[len(streaksAll)] = [
                start_date,
                end_date,
                streak_len,
                streak_gain,
                streak_vol,
            ]

    # appending streak_df_close for the aggregator page graph thumb nail
    if len(streaksAll) >= 1:
        streakLatest = streaksAll.iloc[0]
        dfClose = df.copy()
        dfClose = dfClose[dfClose["date"] >= streakLatest["start_date"]]
        dfClose = dfClose[dfClose["date"] <= streakLatest["end_date"]]
        dfClose = dfClose["close"].to_list()
        dfCloseDummy = len(streaksAll) * [None]
        dfCloseDummy[0] = dfClose
        streaksAll["streak_df_close"] = dfCloseDummy

    return streaksAll, df


def UpdateStreaksAllForSymbol(symbol, symbol_id=None, wise="day"):
    """Update all streaks for the symbol"""
    if symbol_id is None:
        q = f"SELECT id FROM assets WHERE ticker = '{symbol}';"
        symbol_id = db.DfFromDb(q)
        if len(symbol_id) == 1:
            symbol_id = symbol_id.iloc[0]["id"]
        else:
            logger.warning(f"{symbol} not in assets db or multiple symbol entries")
            return None
    # df = fromApi.DailyDfFromAlphaVantage(symbol, outputsize="full")
    # df = db.ReadTableDailyData(symbol, limit=None, idAsset=symbol_id)
    df = fromApi.DailyDfFromFmg(symbol, wise=wise)
    if len(df) == 0:
        return None
    streaksAllUp, df = StreaksAllFromDf(df, streakLenMin=3, direction="up")
    streaksAllDown, df = StreaksAllFromDf(df, streakLenMin=3, direction="down")
    if (streaksAllUp is None) and (streaksAllDown is None):
        streaksall = None
    elif streaksAllUp is None:
        streaksAll = streaksAllDown
    elif streaksAllDown is None:
        streaksAll = streaksAllUp
    elif isinstance(streaksAllUp, pd.DataFrame) and isinstance(
        streaksAllDown, pd.DataFrame
    ):
        streaksAll = pd.concat([streaksAllUp, streaksAllDown], ignore_index=True)
    else:
        logger.error("streaksAll instance not dataframe")
    streaksAll["wise"] = wise
    if len(streaksAll) >= 1:
        streaksAll["symbol_id"] = symbol_id
        db.Streaks2StreaksTable(streaksAll, wise=wise)
    else:
        db.DeleteAllStreaksForSymbolId(symbol_id)

    ds = df.iloc[0]
    db.CloseLastInsert(symbol, ds, idAsset=symbol_id, wise=wise)

    return streaksAll


#def UpdateRunningStreaksAll():
#    """Update all running streaks"""
#    assets = db.ReadTable("assets")
#    failedSymbols = []
#    kStart = 0
#    for k in range(kStart, len(assets)):
#        tic = time.time()
#        asset = assets.iloc[k]
#        symbol = asset["ticker"]
#        symbol_id = asset["id"]
#        try:
#            streak = UpdateStreakLatestForSymbol(symbol, symbol_id=symbol_id)
#            print(50 * "-", symbol, 50 * "-")
#            print(streak)
#        except Exception as e:
#            failedSymbols.append(symbol)
#            logger.warning(
#                "%s UpdateRunningStreaksAll: Failed for %s", e, failedSymbols
#            )
#            # print("Failed UpdateRunningStreaksAll for", symbol, symbol_id)
#            # print("failedSymbols:", failedSymbols)
#        toc = time.time() - tic
#        toFinish = datetime.datetime.now() + datetime.timedelta(
#            seconds=(len(assets) - k) * toc
#        )
#        print(
#            "finishing at:",
#            toFinish,
#            "     toc=",
#            toc,
#            "   track:",
#            k,
#            "/",
#            len(assets),
#            symbol,
#        )
#        print(50 * "-", symbol, 50 * "-")
#    print("failedSymbols:", failedSymbols)


def StreaksSummaryTable(
    streaks, streakRunning=None, forPage="aggregator", direction=None, wise="day"
):
    """This method is expected to return streaks summary table as a pd.DataFrame
        This method is meaningful if streaks passded to this function are filtered and have only one direciton, either all up or all down.
    Args:
        streaks (pd.DataFrame):
    Returns:
        StreaksSummaryTable (pd.DataFrame)
    """
    if wise is None:
        raise ("wise should be proivided for StreaksSummaryTable call")
    columns = [
        "streak_len",
        "occurrences_count",
        "gain_max",
        "gain_mean",
        "gain_running",
        "vol_max",
        "vol_mean",
        "vol_running",
        "gain_min",
        "gain_median",
        "gain_std",
        "gain_max_date",
        "vol_min",
        "vol_median",
        "vol_std",
        "vol_max_date",
        "end_date",
        "start_date",
        "streak_df_close",
        "wise",
    ]
    streaksSummaryTable = pd.DataFrame(columns=columns)
    if forPage == "aggregator":
        if streakRunning is None:
            streakRunning = streaks.iloc[0]
        if streakRunning["streak_gain"] >= 0:
            direction = "up"
            streaks = streaks.loc[streaks["streak_gain"] >= 0.0]
        else:
            direction = "down"
            streaks = streaks.loc[streaks["streak_gain"] <= 0.0]
        streakLengths = [streakRunning["streak_len"]]
    elif forPage == "companywise":
        streakLengths = list(set(streaks["streak_len"]))
    else:
        print(
            50 * "-",
            "check forPage argument, it should be aggregator or companywise",
            50 * "-",
        )
        return None
    for streakLen in streakLengths:
        streak = streaks.loc[streaks["streak_len"] == streakLen]
        resStreakLen = pd.Series(index=columns)
        resStreakLen["streak_len"] = streakLen
        resStreakLen["occurrences_count"] = len(streak)
        resStreakLen["gain_mean"] = streak["streak_gain"].mean()
        if resStreakLen["gain_mean"] >= 0:
            resStreakLen["gain_max"] = streak["streak_gain"].max()
            resStreakLen["gain_min"] = streak["streak_gain"].min()
        else:
            resStreakLen["gain_max"] = streak["streak_gain"].min()
            resStreakLen["gain_min"] = streak["streak_gain"].max()
        resStreakLen["gain_median"] = streak["streak_gain"].median()
        resStreakLenStd = streak["streak_gain"].std()
        if np.isnan(resStreakLenStd):
            resStreakLen["gain_std"] = None
        else:
            resStreakLen["gain_std"] = resStreakLenStd
        resStreakLen["gain_max_date"] = streak.iloc[streak["streak_gain"].argmax()][
            "start_date"
        ]
        resStreakLen["vol_max"] = streak["streak_vol"].max()
        resStreakLen["vol_min"] = streak["streak_vol"].min()
        resStreakLen["vol_mean"] = streak["streak_vol"].mean()
        resStreakLen["vol_median"] = streak["streak_vol"].median()
        resStreakLenStd = streak["streak_vol"].std()
        if np.isnan(resStreakLenStd):
            resStreakLen["vol_std"] = None
        else:
            resStreakLen["vol_std"] = resStreakLenStd
        resStreakLen["vol_max_date"] = streak.iloc[streak["streak_vol"].argmax()][
            "start_date"
        ]

        if streakRunning is not None:
            if streakRunning["streak_len"] == streakLen:
                resStreakLen["gain_running"] = streakRunning["streak_gain"]
                resStreakLen["vol_running"] = streakRunning["streak_vol"]
                resStreakLen["end_date"] = streakRunning["end_date"]
                resStreakLen["start_date"] = streakRunning["start_date"]
                resStreakLen["streak_df_close"] = streakRunning["streak_df_close"]
        # return streaksSummaryTable, resStreakLen
        # streaksSummaryTable = pd.concat([streaksSummaryTable, pd.DataFrame(dict(resStreakLen), index=[0])], axis=0, ignore_index=True)
        resStreakLen["wise"] = wise
        streaksSummaryTable.loc[len(streaksSummaryTable)] = resStreakLen
        # streaksSummaryTable.append(resStreakLen, igonore_index=True)
    return streaksSummaryTable


def StreaksSummaryOfSymbol(symbol, symbol_id=None, direction="", wise="day"):
    """Given a symbol, calculate streaks summary table"""
    if symbol_id is None:
        symbol_id = db.IdAsset(symbol)
    if direction == "up":
        directionCompare = ">="
    elif direction == "down":
        directionCompare = "<="
    else:
        print(
            "direction not given or wrong, check StreaksSummaryOfSymbol, aggregator_data_for_api.py"
        )
        return None
    queryStreaksOfSymbolId = f"""
        SELECT * FROM streaks WHERE symbol_id = {symbol_id} 
        AND streak_gain {directionCompare} 0.0
        AND wise = '{wise}'
        ORDER BY end_date DESC;
        """
    streaksOfSymbol = db.DfFromDb(queryStreaksOfSymbolId)
    streaksOfSymbolSummary = StreaksSummaryTable(
        streaksOfSymbol, forPage="aggregator", direction=direction, wise=wise
    )
    return streaksOfSymbolSummary


def RunningStreaksSummary(direction="", exchange=None, wise="day"):
    """Calulcate streaks summary for all the running streaks"""
    if direction == "up" or direction == "down":
        runningStreaks = db.ReadRunningStreaks(
            direction=direction, exchange=exchange, wise=wise
        )
    else:
        logger.error(
            "direction to be set to up or down in RunningStreaksSummary of aggregator_data_for_api"
        )
        return None
    failedSymbols = []
    if runningStreaks is None:
        logger.warning(f"{exchange} {wise} {direction} No running streaks")
        return None
    for k in range(len(runningStreaks)):
        tic = time.time()
        runningStreak = runningStreaks.iloc[k]
        symbol = runningStreak["symbol"]
        symbol_id = runningStreak["symbol_id"]
        name_asset = runningStreak["name_asset"]
        try:
            streaksSummaryOfSymbol = StreaksSummaryOfSymbol(
                symbol, symbol_id=symbol_id, direction=direction, wise=wise
            )
            streaksSummaryOfSymbol["symbol"] = symbol
            streaksSummaryOfSymbol["symbol_id"] = symbol_id
            streaksSummaryOfSymbol["name_asset"] = name_asset

            # print("streaksSummaryOfSymbol:", streaksSummaryOfSymbol)
            if "streaksSummaries" not in locals():
                streaksSummaries = streaksSummaryOfSymbol.copy()
            else:
                streaksSummaries = pd.concat(
                    [streaksSummaries, streaksSummaryOfSymbol], ignore_index=True
                )
        except KeyboardInterrupt:
            logger.info("%s RunningStreaksSummary interrupted by keyboard", exchange)
            sys.exit()
        except Exception as e:
            logger.warning("%s Failed RunningStreaksSummary, e: %s ", symbol, e)
            failedSymbols.append(symbol)
            # print("failedSymbols:", failedSymbols)
        # print(50 * "-", "streaksSummaryOf", symbol, 50 * "-")
        logger.info("%s %s streaksSummary: %s", symbol, wise, streaksSummaryOfSymbol)
        # print(streaksSummaryOfSymbol)
        toc = time.time() - tic
        toFinish = datetime.timedelta(seconds=(len(runningStreaks) - k) * toc)
    #        print(
    #            symbol,
    #            "finishing in:",
    #            toFinish,
    #            "     toc=",
    #            toc,
    #            "   track:",
    #            k,
    #            "/",
    #            symbol,
    #        )
    # print(50 * "-", symbol, 50 * "-")
    if len(failedSymbols) > 0:
        logger.warning("failedSymbols: %s", failedSymbols)
    if "streaksSummaries" in locals():
        return streaksSummaries


def RunningStreaksSummary2Db(exchange=None, wise="day"):
    """Calcualte running streaks summary and send to db"""
    runningStreaksSummaryUp = RunningStreaksSummary(
        direction="up", exchange=exchange, wise=wise
    )
    runningStreaksSummaryDown = RunningStreaksSummary(
        direction="down", exchange=exchange, wise=wise
    )
    db.DeleteExchangeFromStreaksRunningSummaryDb(exchange=exchange, wise=wise)
    if (runningStreaksSummaryUp is None) and (runningStreaksSummaryDown is None):
        return None
    elif isinstance(runningStreaksSummaryUp, pd.DataFrame) and isinstance(
        runningStreaksSummaryDown, pd.DataFrame
    ):
        runningStreaksSummaryAll = pd.concat(
            [runningStreaksSummaryUp, runningStreaksSummaryDown], ignore_index=True
        )
    elif isinstance(runningStreaksSummaryUp, pd.DataFrame):
        runningStreaksSummaryAll = runningStreaksSummaryUp
    elif isinstance(runningStreaksSummaryDown, pd.DataFrame):
        runningStreaksSummaryAll = runningStreaksSummaryDown
    else:
        logger.error(
            f"{exchange} {wise} : Check runningStreaksSummaryAll, maybe Up and Down too"
        )
    runningStreaksSummaryAll.to_sql(
        "streaks_running_summary", db.engine, if_exists="append", index=False
    )


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)
