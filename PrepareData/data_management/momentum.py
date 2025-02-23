"""Momentum Features"""

# This script's function here is to update the "raw_data" table with the latest
# market data for the assets in the "assets" table.  It reads a csv file
# cointaining the compact size data from alphavantage and updates the table
# with the latest incoming data.

# pylint: disable=line-too-long
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=C0121 # disable pandas not check, not sure what it mean
# pylint: disable=E1137 # item assignment dictionary, meh
# pylint: disable=E1136 # unsubscriptable

# from . import db
# import db
import pandas as pd

masDefined = (5, 10, 20, 50, 100, 200)


def MomentumMetrics(df, ma=5):
    """Returns momentum metrics along with ma"""
    momentumMetrics = {"up": None, "down": None, "run": None}
    momentumMetrics["up"] = {"run": None, "mean": None, "max": None}
    momentumMetrics["down"] = {"run": None, "mean": None, "max": None}
    momentumMetrics["run"] = {"direction": None, "len": None}

    df = df.copy()
    # df = df.iloc[: -201 + ma]
    df["ma"] = None
    df["up"] = None
    df["groupId"] = None
    if not isinstance(df, pd.DataFrame):
        return df, momentumMetrics
    if ma <= 1:
        return df, momentumMetrics
    if len(df) <= 1:
        return df, momentumMetrics
    df["ma"] = df["close"][::-1].rolling(ma, min_periods=1).mean()[::-1]
    # df = df.iloc[0 : -(ma - 1)]
    df["up"] = df["close"] > df["ma"]
    df["groupId"] = (df["up"] != df["up"].shift(1)).cumsum()
    dfUp = df[df["up"]]
    dfDown = df[df["up"] == False]
    lensUp = dfUp.groupby("groupId").size()
    lensDown = dfDown.groupby("groupId").size()

    momentumMetrics["up"] = {"run": None, "mean": lensUp.mean(), "max": lensUp.max()}
    momentumMetrics["down"] = {
        "run": None,
        "mean": lensDown.mean(),
        "max": lensDown.max(),
    }

    if len(lensUp) > 0:
        if df["up"].iloc[0] == True:
            momentumMetrics["run"]["direction"] = "up"
            momentumMetrics["run"]["len"] = lensUp.iloc[0]
            momentumMetrics["up"]["run"] = lensUp.iloc[0]
            return df, momentumMetrics
    if len(lensDown) > 0:
        if df["up"].iloc[0] == False:
            momentumMetrics["run"]["direction"] = "down"
            momentumMetrics["run"]["len"] = lensDown.iloc[0]
            momentumMetrics["down"]["run"] = lensDown.iloc[0]
            return df, momentumMetrics
    return df, momentumMetrics


def MomentumMetricsAll(df, mas=masDefined):  # , plotForMa=None):
    """Returns all momentum metrics"""
    df = df.copy()
    columnNames = [
        "upRun",
        "upMean",
        "upMax",
        "downRun",
        "downMean",
        "downMax",
        "runDir",
        "runLen",
    ]
    momentumMetricsAll = pd.DataFrame(columns=columnNames, index=mas)
    for ma in mas:
        _df, momentumMetrics = MomentumMetrics(df, ma=ma)
        momentumMetricsAll.loc[ma] = {
            "upRun": momentumMetrics["up"]["run"],
            "upMean": momentumMetrics["up"]["mean"],
            "upMax": momentumMetrics["up"]["max"],
            "downRun": momentumMetrics["down"]["run"],
            "downMean": momentumMetrics["down"]["mean"],
            "downMax": momentumMetrics["down"]["max"],
            "runDir": momentumMetrics["run"]["direction"],
            "runLen": momentumMetrics["run"]["len"],
        }
    return momentumMetricsAll
