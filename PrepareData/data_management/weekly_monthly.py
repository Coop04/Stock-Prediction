#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module has a few function required for weekwise and monthwise streak management.

"""

# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # disable line too long
# pylint: disable=E0401 # disable import error for import db and from_api.
# pylint: disable=C0103 # disable snake_case warning
# pylint: disable=W0718 # disable blanket exception
# pylint: disable=W1203 # disbale lazy formating in logging
# pylint: disable=R0911 # disbale too many returns
import datetime
import numpy as np
import logging
import time
import pandas as pd

if __name__ in ["__main__", "weekly_monthly"]:
    import db
    import from_api as fromApi
else:
    from . import db
    from . import from_api as fromApi

logger = logging.getLogger(__name__)
logger.debug("Starting streaks.py")


def DfResample(df, wise=None, latestTail=False):
    """Resamples to weekly and monthly.
    Params: df : daywise close price df
        wise: "week"/"month/day"
        latestTail: returns running month and week upto if True, default False
    Returns resampled
    """
    dfCopy = df.copy()
    if "date" not in df.columns:
        logger.error("df doesnt have date column")
        return None
    dfCopy["date"] = pd.to_datetime(dfCopy["date"])
    dfCopy.set_index("date", inplace=True)
    if wise == "week":
        # dfResampled = dfCopy.resample("W-FRI").last()
        dfResampled = dfCopy.resample("W-FRI").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        dfResampled = dfResampled[::-1].reset_index()
    elif wise == "month":
        # dfResampled = dfCopy.resample("ME").last()
        dfResampled = dfCopy.resample("ME").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        dfResampled = dfResampled[::-1].reset_index()
    elif wise == "quarter":
        # dfResampled = dfCopy.resample("ME").last()
        dfResampled = dfCopy.resample("Q").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        dfResampled = dfResampled[::-1].reset_index()

    elif wise == "day":
        dfResampled = dfCopy.reset_index()
    else:
        logger.error("wise parameter is niether month nor week")
        return None
    if latestTail == False:
        if dfResampled["date"].iloc[0] > dfCopy.index[0]:
            dfResampled = dfResampled[1:]
    else:
        dfResampled.loc[0, "date"] = pd.to_datetime( df.loc[0, "date"])
    return dfResampled


if __name__ == "__main__":
    df = fromApi.DailyDfFromFmg("INFY.NS", limit=200)
    dfResampled = DfResample(df, wise="month")
    print(dfResampled)
