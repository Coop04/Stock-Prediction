# tests/test_from_api.py

# pylint: disable=E0401 # import error discarded
# pylint: disable=C0103 # disable snake_case warning

"""Tests for from_api.py"""

import datetime

# import pytest

# from data_management import from_api as fromApi
import from_api as fromApi


# data required for tests.
# This can be modified over time, depending on the changes in the API and markets we are interested
EXCHANGE = "NSE"
SYMBOL = "INFY.NS"


def test_BulkDayFmg():
    """Test for BulkDayFmg"""
    date = "2024-09-10"
    print(
        "BulkDayFmg test takes a while a minute to fetch data from API, be patient and wait"
    )
    df = fromApi.BulkDayFmg(date)
    columns = list(df.columns)
    assert len(df) > 60000
    assert columns == [
        "symbol",
        "date",
        "open",
        "low",
        "high",
        "close",
        "adjClose",
        "volume",
    ]


def test_OpenDaysExchange():
    """Test for OpenDaysExchange"""
    openDaysExchange = fromApi.OpenDaysExchange(EXCHANGE)
    assert len(openDaysExchange) == 4
    assert isinstance(openDaysExchange[0], datetime.date)


def test_DailyDfFromFmg():
    """Test for DailyDfFromFmg"""
    df = fromApi.DailyDfFromFmg(SYMBOL, limit=10)
    assert len(df) == 10
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_CompanyOverview():
    """Test for CompanyOverview"""
    companyOverview = fromApi.CompanyOverview(SYMBOL)
    assert list(companyOverview.index) == [
        "symbol",
        "price",
        "beta",
        "volAvg",
        "mktCap",
        "lastDiv",
        "range",
        "changes",
        "companyName",
        "currency",
        "cik",
        "isin",
        "cusip",
        "exchange",
        "exchangeShortName",
        "industry",
        "website",
        "description",
        "ceo",
        "sector",
        "country",
        "fullTimeEmployees",
        "phone",
        "address",
        "city",
        "state",
        "zip",
        "dcfDiff",
        "dcf",
        "image",
        "ipoDate",
        "defaultImage",
        "isEtf",
        "isActivelyTrading",
        "isAdr",
        "isFund",
    ]


def test_Assets():
    """Test for Assets"""
    print("test_Assets, fetching assets from the API takes a while, wait for a minute")
    assets = fromApi.Assets()
    assert len(assets) > 9000
    assert list(assets.columns) == [
        "symbol",
        "name",
        "price",
        "exchange",
        "exchangeShortName",
        "type",
    ]


def test_Rsi():
    """Test for Rsi"""
    rsi = fromApi.Rsi(SYMBOL)
    assert isinstance(rsi, float)
