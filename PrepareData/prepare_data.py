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
Methods to prepare data for AI optimization
"""

import pandas as pd
import numpy as np

if __name__ in ["__main__"]:
    import data_management.data_update_weekly as du
    import data_management.db as db
    import data_management.momentum as momentumMod
    import data_management.from_api as fromApi
    import data_management.streaks as streaksMod
    from logger_ import logger
    
else:
    from . import momentum
    from . import from_api as fromApi
    from .logger_ import logger

streakLenMin = 3
maWindows = [5, 200]

def StreaksAllForAiFromDf(df, direction="up"):
    streaksAllFromDf, df = streaksMod.StreaksAllFromDf(df, streakLenMin=streakLenMin, direction=direction)
    # print(streaksAllFromDf.columns)
    try:
        streaksAllFromDf.drop("streak_df_close", axis=1, inplace=True)
    except:
        pass
    streaksAllForAi = streaksAllFromDf.copy()

    for k in range(len(streaksAllFromDf)):
        row = streaksAllFromDf.iloc[k]
        if row["streak_len"] > streakLenMin:
            dfStreakTillEnd = df [(df["date"] >= row["start_date"]) & (df["date"] <= row["end_date"])]
            for kExcess in range(streakLenMin, row["streak_len"]):
                dfStreakSub = dfStreakTillEnd.iloc[-kExcess:]
                start_date = dfStreakSub.iloc[-1]["date"]
                end_date = dfStreakSub.iloc[0]["date"]
                streak_len = kExcess
                streak_gain = dfStreakSub["gainPerc"].sum()
                streak_vol = dfStreakSub["volume"].sum()
                streakPartial = {"start_date": start_date, "end_date": end_date, "streak_len": kExcess, "streak_gain": streak_gain, "streak_vol": streak_vol}
                # streakPartial = pd.Series(streakPartial)
                streaksAllForAi.loc[streaksAllForAi.index.max()+1] = streakPartial
    streaksAllForAi.reset_index(drop=True, inplace=True)
    return streaksAllForAi, df

def SortStreaks(streaks):
    streaks.sort_values(by="end_date", ascending=False, inplace=True)
    streaks.reset_index(drop=True, inplace=True)
    return streaks
            
def Occurrences(streaks):
    streaks.reset_index(drop=True, inplace=True)
    streaks["occurrence"] = None
    streaks["gain_mean"] = None
    streaks["vol_mean"] = None
    for k in range(len(streaks)):
        streak_len = streaks.iloc[k]["streak_len"]
        # streaksSub = streaks.iloc[k+1:] [streaks.iloc[k+1:]["streak_len"] == streak_len]
        # streaksSub for cumulative previous measures inlcude the running streak too for avoiding NaN in case the streak is first of it's kind
        streaksSub = streaks.iloc[k:] [streaks.iloc[k:]["streak_len"] == streak_len]
        
        occurrence = len(streaksSub)
        streaks.loc[k, "occurrence"] = occurrence

        gain_mean = streaksSub["streak_gain"].mean()
        streaks.loc[k, "gain_mean"] = gain_mean

        vol_mean = streaksSub["streak_vol"].mean()
        streaks.loc[k, "vol_mean"] = vol_mean

        # performance = streaks.iloc[k+1:] [streaks.iloc[k+1]:] ["streak_len"] == 
        # streaks.iloc[k]["occurrence"] = occurrence
        # print(k)
    return streaks

def MaMetrics(streaks, df):
    streaks.reset_index(drop=True, inplace=True)
    for kWin in maWindows:
        streaks[f"ma{kWin}upRun"] = 0.0
        streaks[f"ma{kWin}upMean"] = 0.0
    for kWin in maWindows:
        streaks[f"ma{kWin}downRun"] = 0.0
        streaks[f"ma{kWin}downMean"] = 0.0
#    streaks["ma5p"] = 0
#    streaks["ma10p"] = 0
#    streaks["ma5n"] = 0
#    streaks["ma10n"] = 0
    for k in range(len(streaks)):
        end_date = streaks.iloc[k]["end_date"]
        dfSub = df[ df["date"] <= end_date]
        momentumMetricsAll = momentumMod.MomentumMetricsAll(dfSub, maWindows)   
        for kWin in maWindows:
            # print("kWin:", kWin)
            # print(momentumMetricsAll)
            momentumMetrics = momentumMetricsAll.loc[kWin]
            if momentumMetrics["upRun"] is not None:
                colName = f"ma{kWin}upRun"
                streaks.loc[k, colName] = momentumMetrics["upRun"]
                colName = f"ma{kWin}upMean"
                streaks.loc[k, colName] = momentumMetrics["upMean"]
            else:
                colName = f"ma{kWin}downRun"
                streaks.loc[k, colName] = momentumMetrics["downRun"]
                colName = f"ma{kWin}downMean"
                streaks.loc[k, colName] = momentumMetrics["downMean"]
    return streaks

def SpreadSheetBooleanVerbose(streaks):
    spreadSheet = pd.DataFrame()
    spreadSheet["date"] = streaks["end_date"]
    spreadSheet["company"] = streaks["symbol"]
    spreadSheet["streak_len"] = streaks["streak_len"]

    spreadSheet["direction"] = "0"
    spreadSheet.loc[streaks["streak_gain"] > 0.0, "direction" ] = "up"
    spreadSheet.loc[streaks["streak_gain"] < 0.0, "direction" ] = "down"

    spreadSheet["occurrence"] = streaks["occurrence"]

    spreadSheet["performance"] = "0"
    spreadSheet.loc[streaks["streak_gain"] > streaks["gain_mean"], "performance" ] = "higher"
    spreadSheet.loc[streaks["streak_gain"] < streaks["gain_mean"], "performance" ] = "lower"

    spreadSheet["vol"] = "0"
    spreadSheet.loc[streaks["streak_vol"] > streaks["vol_mean"], "vol" ] = "higher"
    spreadSheet.loc[streaks["streak_vol"] < streaks["vol_mean"], "vol" ] = "lower"

    for kWin in maWindows:
        spreadSheet[f"ma{kWin}_pos"] = "0"
    for kWin in maWindows:
        spreadSheet[f"ma{kWin}_neg"] = "0"

    for kWin in maWindows:
        colRun = f"ma{kWin}upRun"
        colMean = f"ma{kWin}upMean"
        colRes = f"ma{kWin}_pos"
        spreadSheet.loc[ streaks[colRun] > streaks[colMean], colRes] = "higher"
        spreadSheet.loc[ streaks[colRun] < streaks[colMean], colRes] = "lower"

    for kWin in maWindows:
        colRun = f"ma{kWin}downRun"
        colMean = f"ma{kWin}downMean"
        colRes = f"ma{kWin}_neg"
        spreadSheet.loc[ streaks[colRun] > streaks[colMean], colRes] = "higher"
        spreadSheet.loc[ streaks[colRun] < streaks[colMean], colRes] = "lower"

    spreadSheet["next_day_actual"] = "0"
    spreadSheet.loc[streaks["next_day_actual"] > 0.0, "next_day_actual" ] = "higher"
    spreadSheet.loc[streaks["next_day_actual"] < 0.0, "next_day_actual" ] = "lower"

    return spreadSheet

def SpreadSheetBoolean(streaks):
    spreadSheet = pd.DataFrame()
    spreadSheet["date"] = streaks["end_date"]
    spreadSheet["symbol"] = streaks["symbol"]
    spreadSheet["streak_len"] = streaks["streak_len"]

    spreadSheet["direction"] = 0
    spreadSheet.loc[streaks["streak_gain"] > 0.0, "direction" ] = 1
    spreadSheet.loc[streaks["streak_gain"] < 0.0, "direction" ] = -1

    spreadSheet["occurrence"] = streaks["occurrence"]

    spreadSheet["performance"] = 0
    spreadSheet.loc[streaks["streak_gain"] > streaks["gain_mean"], "performance" ] = 1
    spreadSheet.loc[streaks["streak_gain"] < streaks["gain_mean"], "performance" ] = -1

    spreadSheet["vol"] = 0
    spreadSheet.loc[streaks["streak_vol"] > streaks["vol_mean"], "vol" ] = 1
    spreadSheet.loc[streaks["streak_vol"] < streaks["vol_mean"], "vol" ] = -1

    for kWin in maWindows:
        spreadSheet[f"ma{kWin}_pos"] = 0
    for kWin in maWindows:
        spreadSheet[f"ma{kWin}_neg"] = 0

    for kWin in maWindows:
        colRun = f"ma{kWin}upRun"
        colMean = f"ma{kWin}upMean"
        colRes = f"ma{kWin}_pos"
        spreadSheet.loc[ streaks[colRun] > streaks[colMean], colRes] = 1
        spreadSheet.loc[ streaks[colRun] < streaks[colMean], colRes] = -1

    for kWin in maWindows:
        colRun = f"ma{kWin}downRun"
        colMean = f"ma{kWin}downMean"
        colRes = f"ma{kWin}_neg"
        spreadSheet.loc[ streaks[colRun] > streaks[colMean], colRes] = 1
        spreadSheet.loc[ streaks[colRun] < streaks[colMean], colRes] = -1

    spreadSheet["next_day_actual"] = 0
    spreadSheet.loc[streaks["next_day_actual"] > 0.0, "next_day_actual" ] = 1
    spreadSheet.loc[streaks["next_day_actual"] < 0.0, "next_day_actual" ] = -1

    return spreadSheet

def NextDayActual(streaks, df): 
    streaks["next_day_actual"] = 0.0
    streaks.reset_index(drop=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    for k in range(len(streaks)):
        index = df [ df["date"] == streaks.iloc[k]["end_date"]].index.values[0]
        if index > 0:
            index = index - 1
            nextDayActual = df.loc[index, "gainPerc"]
            streaks.loc[k, "next_day_actual"] = nextDayActual
    return streaks

def PrepareDataForTraining(symbols, wise="day", limit=100):
    directions = ["up", "down"]
    streaksAllForTraining = []
    kCount = 0
    for symbol in symbols:
        print(f"{kCount}/{len(symbols)} | {symbol}")
        kCount = kCount + 1
        try:
            df = fromApi.DailyDfFromFmg(symbol, limit=limit, wise=wise, latestTail=latestTail)
            for direction in directions:
                streaksAllForAi, df = StreaksAllForAiFromDf (df, direction=direction)
                streaksAllForAi = SortStreaks(streaksAllForAi)
                streaksAllForAi = Occurrences(streaksAllForAi)
                streaksAllForAi = MaMetrics(streaksAllForAi, df)
                streaksAllForAi["symbol"] = symbol
                streaksAllForAi = NextDayActual(streaksAllForAi, df)
                streaksAllForTraining.append(streaksAllForAi)
        except:
            print(f"{symbol} failed")
    streaksAllForTraining = pd.concat(streaksAllForTraining)
    streaksAllForTraining = SortStreaks(streaksAllForTraining)

    spreadSheetBoolean = SpreadSheetBoolean(streaksAllForTraining)
    return spreadSheetBoolean

def BinSimilar(spreadSheetBoolean):
    # featuresList = ["streak_len", "direction", "occurrence", "performance"]
    featuresList = ["streak_len", "direction", "performance", "performance", "vol", "ma5_pos", "ma200_pos", "ma5_neg", "ma200_neg"]
    spreadSheetBoolean["bin_name"] = spreadSheetBoolean[featuresList].astype(str).apply(" ".join, axis=1)
    return spreadSheetBoolean

def BinSum(spreadSheetBoolean, binName):
    # binnedDict = {}
    binSub = spreadSheetBoolean[ spreadSheetBoolean["bin_name"] == binName]
    binnedSeries = binSub.iloc[0] 
    binnedSeries = binnedSeries.copy()
    binnedSeries["next_day_pos_count"] = len(binSub [binSub["next_day_actual"] == 1])
    binnedSeries["next_day_neg_count"] = len(binSub [binSub["next_day_actual"] == -1])
    return binnedSeries

def BinSums(spreadSheetBoolean):
    binNames = list(set(spreadSheetBoolean["bin_name"]))
    binnedSheet = []
    for binName in binNames:
        binnedSeries = BinSum(spreadSheetBoolean, binName)
        binnedSheet.append(binnedSeries)
    binnedSheet = pd.DataFrame(binnedSheet)
    return binnedSheet

def HistoricalBinName(spreadSheetBoolean, binName):
    binSub = spreadSheetBoolean [spreadSheetBoolean["bin_name"] == binName]
    return binSub





if __name__ == "__main__":
    symbol = "INFY.NS"
    symbol = "JSWHL.NS"
    symbol = "ONGC.NS"
    wise = "day"
    direction = "down"
    latestTail = False
    limit = 200

    symbols = ["INFY.NS", "TSLA", "MSFT"]
    # symbols = ["INTC"]
    # symbols = ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "ADBE", "PEP"]
    # symbols = du.AssetsToUpdate("NASDAQ")
    # symbols = list(symbols["symbol"])

    # spreadSheetBoolean = PrepareDataForTraining(symbols, wise="day", limit=1000)
    # spreadSheetBoolean = PrepareDataForTraining(symbols, wise="day", limit=100000)
    spreadSheetBoolean = PrepareDataForTraining(symbols, wise="day", limit=None)
    spreadSheetBoolean = BinSimilar(spreadSheetBoolean)
    binNames = list(set(spreadSheetBoolean["bin_name"]))

    binnedSheet = BinSums(spreadSheetBoolean)

    binnedSheet["ratio"] = binnedSheet["next_day_pos_count"] / binnedSheet["next_day_neg_count"]
    binnedSheet = binnedSheet.sort_values("ratio", ascending=False)
    
    binnedSheetValid = binnedSheet.replace([np.inf, -np.inf], np.nan).dropna()


    spreadSheetBoolean.to_csv("prepared_data.csv", index=False)
    # limit = None
#    df = fromApi.DailyDfFromFmg(symbol, limit=limit, wise=wise, latestTail=latestTail)
#    streaksAllForAi, df = StreaksAllForAiFromDf (df, direction=direction)
#    streaksAllForAi = SortStreaks(streaksAllForAi)
#    streaksAllForAi = Occurrences(streaksAllForAi)
#    streaksAllForAi = MaMetrics(streaksAllForAi, df)
#    streaksAllForAi["symbol"] = symbol
#    streaksAllForAi = NextDayActual(streaksAllForAi, df)
#    spreadSheetBoolean = SpreadSheetBoolean(streaksAllForAi)
#
#    spreadSheetBoolean.to_excel("preapred_data.xlsx", index=False)

