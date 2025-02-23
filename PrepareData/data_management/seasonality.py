# pylint: disable=all
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # Your long line of code goes here
# pylint: disable=C0103 # Disbling sname_case warning
# pylint: disable=E0401 # import error discard
# pylint: disable=W0718 # General exception catching e
# pylint: disable=W1203 # f" lazy % formatting

"""
seasonality.py
This module provides the seasonality core functionalities, to be called by companywise.py
"""

# import pdb
import sys
import humanize
import datetime
import pandas as pd
import numpy as np
import yaml
import requests
import traceback

if __name__ in ["__main__", "seasonality"]:
    from logger_ import logger
else:
    from .logger_ import logger


def WiseColumnDf(df, wise="month"):
    seasonalityDf = None
    if df is None:
        return None
    if len(df) == 0:
        return None
    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    df["diff"] = df["close"].diff(periods=-1)
    df["gainPerc"] = 100 * df["diff"].iloc[0:-1] / df["close"].iloc[1:].values
    df["up"] = df["diff"] > 0.0
    df["down"] = df["diff"] < 0.0

    # Convert the 'date' column to datetime if it's not already
    df['date'] = pd.to_datetime(df['date'])

    if wise == "month":
        df['month'] = df['date'].dt.strftime('%b')
        while df.iloc[0]["month"] != "Dec":
            df = df.iloc[1:].reset_index(drop=True)
        while df.iloc[-1]["month"] != "Jan":
            df = df.iloc[:-1].reset_index(drop=True)


    elif wise == "quarter":
        df['quarter'] = 'Q' + df['date'].dt.quarter.astype(str)
        while df.iloc[0]["quarter"] != "Q4":
            df = df.iloc[1:].reset_index(drop=True)
        while df.iloc[-1]["quarter"] != "Q1":
            df = df.iloc[:-1].reset_index(drop=True)

    else:
        return None
    
    return df

def PivotedDf(df, wise="month"):

        # Extract year from the 'date' column and add it as a new column
        df['year'] = pd.to_datetime(df['date']).dt.year

        # Pivot the DataFrame to have 'year' as index and quarters as columns
        dfPivoted = df.pivot(index='year', columns=wise, values='gainPerc')

        # Rename columns to 'Q1', 'Q2', 'Q3', 'Q4' if not automatically assigned
        dfPivoted = dfPivoted.rename_axis(None, axis=1)

        return dfPivoted

def MaxMinDf(dfPivoted, maxMin = "max", wise="month"):

    # Create a copy of the original DataFrame
    max_indicator_df = dfPivoted.copy()


    # Broadcast the maximum values across columns for each row
    if maxMin == "max":
        max_values = dfPivoted.max(axis=1).values[:, None]  # Reshape to match dimensions for comparison
    elif maxMin == "min":
        max_values = dfPivoted.min(axis=1).values[:, None]  # Reshape to match dimensions for comparison
    else:
        return None 

    # Create a DataFrame where the maximum value in each row is marked as 1 and others as 0
    max_indicator_df = (dfPivoted.values == max_values).astype(int)
    max_indicator_df = pd.DataFrame(max_indicator_df, index=dfPivoted.index, columns=dfPivoted.columns)


    return max_indicator_df

def SeasonalityDf(df, wise="month"):
    wiseColumnDf = WiseColumnDf(df, wise=wise)
    pivotedDf = PivotedDf(wiseColumnDf, wise=wise)

    if wise == "month":
        # Reordering columns from January to December
        season_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    elif wise == "quarter":
        season_order = ["Q1", "Q2", "Q3", "Q4"]

    pivotedDf = pivotedDf[season_order]
    # print(pivotedDf)

    maxDf = MaxMinDf(pivotedDf, maxMin="max", wise=wise)
    minDf = MaxMinDf(pivotedDf, maxMin="min", wise=wise)
    maxCount = maxDf.sum()
    minCount = minDf.sum()
    seasonalityDf = maxCount.copy()
    seasonalityDf = pd.DataFrame(seasonalityDf)
    seasonalityDf.columns = ["high"]
    # return seasonalityDf
    seasonalityDf["low"] = minCount

    yearsRange = (wiseColumnDf["year"].min(), wiseColumnDf["year"].max())

    return seasonalityDf, yearsRange
    # max_indicator_df 
