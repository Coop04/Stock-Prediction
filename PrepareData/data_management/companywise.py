# pylint: disable=all
# pylint: disable=C0303 # disable trailing whitespace warning
# pylint: disable=C0301 # Your long line of code goes here
# pylint: disable=C0103 # Disbling sname_case warning
# pylint: disable=E0401 # import error discard
# pylint: disable=W0718 # General exception catching e
# pylint: disable=W1203 # f" lazy % formatting

"""
companywise.py
This module take care of APIs for the companywise page, not the final API, but function for creating APIs
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

from bokeh.plotting import figure

from bokeh.models import LinearAxis, DatetimeTickFormatter, WheelZoomTool
from bokeh.models import ColumnDataSource
from bokeh.models import HoverTool

if __name__ in ["__main__"," __file__"]:
    import db
    import momentum
    import from_api as fromApi
    import seasonality as seasonality
    from logger_ import logger
    import yearToDate as ytd
else:
    from . import db
    from . import momentum
    from . import from_api as fromApi
    from . import seasonality as seasonality
    from .logger_ import logger
    from . import yearToDate as ytd
# import momentum
# import db
# import logging

# logger = logging.getLogger(__name__)
# logger.debug("Starting streaks.py")

with open("../config.yml", "r", encoding="utf-8") as f:
    config_data = yaml.safe_load(f)

apikey = config_data["alphavantage"]["apikey"]
url = "https://www.alphavantage.co/query?"


def CandlePlot(p, df):
    """
    Create bokeh candle plot
    Args:
        p (bokeh filgure)
        df (pd.DataFrame):
    Returns:
        p (bokeh figure): which has candle plot
    """
    df["date"] = pd.to_datetime(df["date"])
    df = df.copy()
    inc = df.close > df.open
    dec = df.open > df.close

    xWidth = df["date"].max() - df["date"].min()
    xWidth = xWidth.total_seconds()
    xWidth = xWidth * 1000.0

    w = 0.5 * xWidth / len(df)

    # w = 12 * 60 * 60 * 1000  # half day in ms
    # w = 0.5 * p.width / len(df)
    # w = w * 100

    source_inc = ColumnDataSource(df.loc[inc])
    source_dec = ColumnDataSource(df.loc[dec])

    # wicks
    # Create segments and vbars for increasing and decreasing candles
    p.segment("date", "high", "date", "low", color="#2BA528", source=source_inc)
    vbar_inc = p.vbar(
        "date",
        w,
        "open",
        "close",
        fill_color="#2BA528",
        line_color="#2BA528",
        source=source_inc,
    )
    p.segment("date", "high", "date", "low", color="#F2583E", source=source_dec)
    vbar_dec = p.vbar(
        "date",
        w,
        "open",
        "close",
        fill_color="#F2583E",
        line_color="#F2583E",
        source=source_dec,
    )
    p.yaxis.visible = False
    # p.line(df["date"], df["close"], line_width=2, line_color="#f5f5f5")

    # Create a new y-axis with the same range as the original y-axis
    right_yaxis = LinearAxis(y_range_name="right_axis", axis_label=None)
    p.add_layout(right_yaxis, "right")

    # Link the new y-axis range to the existing y-axis range
    p.extra_y_ranges = {"right_axis": p.y_range}

    p.xaxis.axis_line_color = None
    p.yaxis.axis_line_color = None
    p.xaxis.formatter = DatetimeTickFormatter(
        days="%d-%b-%Y", months="%b %Y", years="%Y"
    )
    # p.xgrid.grid_line_color = None
    # p.ygrid.grid_line_color = None
    p.yaxis.major_tick_line_color = None  # Remove major ticks
    p.yaxis.minor_tick_line_color = None  # Remove minor ticks
    # Add hover tool
    hover = HoverTool(
        tooltips=[
            ("Date", "@date{%F}"),
            # ("Open", "@open{0.2f}"),
            ("Close", "@close{0.2f}"),
            # ("High", "@high{0.2f}"),
            # ("Low", "@low{0.2f}")
            ("Vol Million", "@volume{0.2f}"),
        ],
        formatters={"@date": "datetime"},
        mode="vline",
        renderers=[vbar_inc, vbar_dec],
    )
    p.add_tools(hover)
    return p


def StreaksPlot(p, streaks):
    """
    Plot streaks in the figure p
    Args:
        p: bokeh plot:
        streaks (pd.DataFrame):
    Returns:
        p:
    """
    source = ColumnDataSource(streaks)
    try:
        if streaks.iloc[0]["direction"] == "down":
            color = "#930d0d"
        else:
            color = "#2BA528"
    except Exception as e:
        logger.error(f"Error in streaks plot, {e}")
        color = "#930d0d"
    vstrip_renderer = p.vstrip(
        x0="startDate",
        x1="endDate",
        color=color,
        alpha=0.4,
        source=source,
        hover_fill_color=color,
    )
    hover = HoverTool(
        renderers=[vstrip_renderer],
        tooltips=[
            ("Performance %", "@streakGain{0.00}"),
            ("Volume Million", "@streakVolume{0.00}"),
            ("Start", "@startDate{%F}"),
            ("End", "@endDate{%F}"),
        ],
        formatters={"@startDate": "datetime", "@endDate": "datetime"},
        # visible = False,
        point_policy="follow_mouse",
    )
    p.add_tools(hover)
    return p


def P():
    """
    Creates bokeh figure object with required properties
    Returns:
        p: bokeh figure object
    """
    TOOLS = "pan, box_zoom,reset"
    p = figure(x_axis_type="datetime", tools=TOOLS, width=1273, height=430, title=None)
    p.toolbar.logo = None
    wheel_zoom_tool = WheelZoomTool(dimensions="width")
    p.add_tools(wheel_zoom_tool)
    p.yaxis.visible = False
    # p.line(df["date"], df["close"], line_width=2, line_color="#f5f5f5")

    # Create a new y-axis with the same range as the original y-axis
    right_yaxis = LinearAxis(y_range_name="right_axis", axis_label=None)
    p.add_layout(right_yaxis, "right")

    # Link the new y-axis range to the existing y-axis range
    p.extra_y_ranges = {"right_axis": p.y_range}

    p.xaxis.axis_line_color = None
    p.yaxis.axis_line_color = None
    p.xaxis.formatter = DatetimeTickFormatter(
        days="%d-%b-%Y", months="%b %Y", years="%Y"
    )
    # p.xgrid.grid_line_color = None
    # p.ygrid.grid_line_color = None
    p.yaxis.major_tick_line_color = None  # Remove major ticks
    p.yaxis.minor_tick_line_color = None  # Remove minor ticks
    p.yaxis.major_label_text_font = "ubuntu"
    p.xaxis.major_label_text_font = "ubuntu"

    return p


def CompanyWisePlot(df, streaks):
    """
    Call all the plot fuctions to create the final figure
    """
    p = P()
    if df is None:
        return p
    if isinstance(streaks, pd.DataFrame):
        if len(streaks) > 0:
            p = StreaksPlot(p, streaks)
    p = CandlePlot(p, df)
    p.yaxis.major_label_text_font = "ubuntu"
    p.xaxis.major_label_text_font = "ubuntu"
    # show(p)
    return p


def MomentumPlot(df):
    """
    Plot momentum
    Args:
        df
    Returns:
        p
    """
    p = P()
    if not isinstance(df, pd.DataFrame):
        return p
    yLower = df[["close", "ma"]].min(axis=1)
    yUpper = df[["close", "ma"]].max(axis=1)

    sourceClose = ColumnDataSource(df)
    p.varea(x=df["date"], y1=df["ma"], y2=yLower, fill_color="#930d0d")
    p.varea(x=df["date"], y1=df["ma"], y2=yUpper, fill_color="#2BA528")
    renderLine = p.line(
        "date", "close", line_width=1, line_color="black", source=sourceClose
    )
    # renderLine = p.line(df["date"], df["close"], line_width=2, line_color="black", source=sourceClose)

    hover = HoverTool(
        tooltips=[
            ("Date", "@date{%F}"),
            # ("Open", "@open{0.2f}"),
            ("Close", "@close{0.2f}"),
            # ("High", "@high{0.2f}"),
            # ("Low", "@low{0.2f}")
            ("Vol Million", "@volume{0.2f}"),
        ],
        formatters={"@date": "datetime"},
        mode="vline",
        renderers=[renderLine],
    )
    p.add_tools(hover)

    return p


def StreaksSummaryTable(streaks, streakRunning):
    """This method is expected to return streaks summary table as a pd.DataFrame
        This method is meaningful if streaks passded to this function are filtered and have only one direciton, either all up or all down.
    Args:
        streaks (pd.DataFrame):
    Returns:
        StreaksSummaryTable (pd.DataFrame)
    """
    columns = [
        "streakLen",
        "occurrencesCount",
        "gainMax",
        "gainMean",
        "gainRunning",
        "volMax",
        "volMean",
        "volRunning",
        "crashMax",
        "crashMean",
    ]
    streaksSummaryTable = pd.DataFrame(columns=columns)

    streakLengths = list(set(streaks["streakLen"]))
    for streakLen in streakLengths:
        streak = streaks[streaks["streakLen"] == streakLen]
        resStreakLen = pd.Series(index=columns)
        resStreakLen["streakLen"] = streakLen
        resStreakLen["occurrencesCount"] = len(streak)
        resStreakLen["gainMean"] = streak["streakGain"].mean()
        if resStreakLen["gainMean"] >= 0:
            resStreakLen["gainMax"] = streak["streakGain"].max()
            resStreakLen["gainMin"] = streak["streakGain"].min()
        else:
            resStreakLen["gainMax"] = streak["streakGain"].min()
            resStreakLen["gainMin"] = streak["streakGain"].max()
        resStreakLen["gainMedian"] = streak["streakGain"].median()
        resStreakLenStd = streak["streakGain"].std()
        if isinstance(resStreakLenStd, float):
            resStreakLenStd = round(resStreakLenStd, 2)
        if np.isnan(resStreakLenStd):
            resStreakLen["gainStd"] = ""
        else:
            resStreakLen["gainStd"] = resStreakLenStd
        # resStreakLen["gainMaxDate"] = streak.iloc[streak["streakGain"].argmax()]["startDate"].date()
        resStreakLen["gainMaxDate"] = streak.iloc[streak["streakGain"].argmax()][
            "startDate"
        ]

        resStreakLen["volMax"] = streak["streakVolume"].max()
        resStreakLen["volMin"] = streak["streakVolume"].min()
        resStreakLen["volMean"] = streak["streakVolume"].mean()
        resStreakLen["volMedian"] = streak["streakVolume"].median()
        resStreakLenStd = streak["streakVolume"].std()
        if isinstance(resStreakLenStd, float):
            resStreakLenStd = round(resStreakLenStd, 2)
        if np.isnan(resStreakLenStd):
            resStreakLen["volStd"] = ""
        else:
            resStreakLen["volStd"] = resStreakLenStd
        # resStreakLen["volMaxDate"] = streak.iloc[streak["streakVolume"].argmax()]["startDate"].date()
        resStreakLen["volMaxDate"] = streak.iloc[streak["streakVolume"].argmax()][
            "startDate"
        ]

        resStreakLen["crashMax"] = streak["crashGain"].max()
        resStreakLen["crashMin"] = streak["crashGain"].min()
        resStreakLen["crashMean"] = streak["crashGain"].mean()
        resStreakLen["crashMedian"] = streak["crashGain"].median()
        resStreakLenStd = streak["crashGain"].std()
        if isinstance(resStreakLenStd, float):
            resStreakLenStd = round(resStreakLenStd, 2)
        if np.isnan(resStreakLenStd):
            resStreakLen["crashStd"] = ""
        else:
            resStreakLen["crashStd"] = resStreakLenStd
        resStreakLen["crashMaxDate"] = streak.iloc[streak["crashGain"].argmax()][
            "startDate"
        ]

        if streakRunning is not None:
            if streakRunning["streakLen"] == streakLen:
                resStreakLen["gainRunning"] = streakRunning["streakGain"]
                resStreakLen["volRunning"] = streakRunning["streakVolume"]

        streaksSummaryTable = pd.concat(
            [streaksSummaryTable, pd.DataFrame(dict(resStreakLen), index=[0])],
            axis=0,
            ignore_index=True,
        )
        # streaksSummaryTable.append(resStreakLen, igonore_index=True)
    return streaksSummaryTable


def Streaks2Disp(streaks, streakLen):
    """
    Decides the streak to be displayed in the companywise page, if streaklength is not mentioned
    """
    if streakLen is None:
        return None
    streaks2Disp = streaks[streaks["streakLen"] == streakLen]
    return streaks2Disp


def CompanyOverview(symbol):
    """Retuns the company information, financial ratios, and other key metrics for the equity specified. Data is generally refreshed on the same day a company reports its latest earnings and financials.
        inputs should be case insensitive
    Args:
        symbol (str): symbol of the asset/company/index like GOOG

    Retuns
        companyOverview:
    """
    q = f"SELECT assets.name, assets.sector, market.market_short_name, market.currency, assets.logo_url FROM assets JOIN market ON assets.market = market.market_id WHERE assets.ticker = '{symbol}';"
    try:
        companyOverview = db.DfFromDb(q)
        companyOverview = companyOverview.iloc[-1]
    except Exception as e:
        logger.info(e)
        companyOverview = None
    return companyOverview


def RunningNoneMessage(streaks, dateLatestFromDf, wise="day"):
    """Return message to be displayed in streak summary if there are no running streaks"""
    if not isinstance(streaks, pd.DataFrame):
        return None
    if len(streaks) == 0:
        runningNoneMessage = f"No streak found"
        return runningNoneMessage
    if not isinstance(dateLatestFromDf, datetime.date):
        dateLatestFromDf = dateLatestFromDf.date()
    streakLatest = streaks[streaks["endDate"] == streaks["endDate"].max()]
    streakLatest = streakLatest.iloc[0]
    endDate = streakLatest["endDate"]
    ago = endDate - dateLatestFromDf  # this line might be wrong
    ago = humanize.naturaldelta(ago)
    streakLen = streakLatest["streakLen"]
    if streakLatest["streakGain"] < 0:
        direction = "Down"
    else:
        direction = "Up"
    runningNoneMessage = f"Last Streak: {ago} ago, for {streakLen} {wise}s {direction}"
    return runningNoneMessage


def _Crashes(df, streaks, d):
    """Private method to calculate crashGain.
    d: integer, 1,2, or 3, usually, 0 and negative not accepted
    returns streaks with new crashGain column"""

    def Crash(df, streak, d):
        """Method to caluclate crashGain for a streak"""
        try:
            index = df[df["date"] == streak["endDate"]]
            if len(index) > 0:
                index = index.index[0]
                if index < d:
                    return None
                r = range(index - d, index)
                dfCrash = df.loc[r]
                crashGain = dfCrash["gainPerc"].sum()
                return crashGain
            else:
                logger.warning(
                    f"Something is not right at def Crash, streak: {streak}, df: {df}, d: {d})"
                )
                return None
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(
                f"Error occurred at line {exc_tb.tb_lineno} in file {exc_tb.tb_frame.f_code.co_filename}: {e}"
            )
            # logger.error(f"Error occurred at line {traceback.extract_stack()[-1].lineno} in file {traceback.extract_stack()[-1].filename}: {e}, streak: {streak}, df: {df}, d: {d})")

    if not isinstance(df, pd.DataFrame):
        logger.error(f"df is not dataframe for def Crash")
        return streaks
    if not isinstance(streaks, pd.DataFrame):
        logger.error(f"streaks is not dataframe for def Crash")
        return streaks
    if d <= 0:
        logger.error(f"{d} d negative for def Crash")
        return streaks
    df = df.copy()
    df["diff"] = df["close"].diff(periods=-1)
    df["gainPerc"] = 100 * df["diff"].iloc[0:-1] / df["close"].iloc[1:].values
    streaks["crashGain"] = None
    for index, streak in streaks.iterrows():
        crashGain = Crash(df, streak, d)
        streaks.loc[index, "crashGain"] = crashGain
    return streaks


def CompanywiseDataAll(
    symbol,
    exchange=None,
    direction=None,
    streakLen=None,
    periodLen=None,
    afterStreakDay=1,
    wise="day",
):
    """Function returns all the data and plot required for the companywise page.
    Args:
        symbol (str): symbol of the asset/company/index like GOOG
        exchange (str): stockexchange like NASDAQ
        direction (str, optional): direction of the streak to search for, if None, running streak returned. If no running streak, up streak returned
        streakLen (int, optional): length of the streak interested. If None, running streak is returned, if no running streak, max streakLen streaks are returned.
        periodLen (str, optional): 1M, 3M, 6M,1Y, 5Y, MAX are the valid inputs, if not given, 1M assumed
        afterStreaksDay (str, optional): day after streak on which the streak reversal performance is requested.
        wise (str, optional: day/week/month): default day
    Retuns
        streakSummaryTable (df): summary of all the streaks of a company for the period, and direction.
        streakSummary (pd.Series): Summary of the streak viewed, data for the companywise page top right corners metrices.
        p (bokeh plot): plot of the streaks and candle
    """
    streakRunning = None
    df = None
    streaks = None
    streaksSummaryTable = None
    streakSummary = None
    idAsset = db.IdAsset(symbol)

    # periodLens= {"1M":30, "3M":91, "6M":182, "1Y":365, "5Y":(365 * 5) + 1, "MAX":None, "ALL":None}
    periodLens = {
        "1M": 30,
        "3M": 91,
        "6M": 182,
        "1Y": 366,
        "2Y": 366 * 2,
        "3Y": 366 * 3,
        "5Y": (365 * 5) + 2,
        "7Y": 366 * 7,
        "10Y": 366 * 10,
        "15Y": 366 * 15,
        "20Y": 366 * 20,
        "MAX": None,
        "ALL": None,
    }
    if periodLen in periodLens:
        periodLen = periodLens[periodLen]
    else:
        periodLen = 366 * 10

    # df = db.ReadTableDailyData(symbol, limit=periodLen, idAsset=None)
    df = fromApi.DailyDfFromFmg(symbol, limit=periodLen, wise=wise, latestTail=True)

    logger.debug(f"{symbol}, length of df = {len(df)}")
    if not isinstance(df, pd.DataFrame):
        p = P()
        logger.warning(f"No data returned by API for {symbol} by fromAPI")
        return streaksSummaryTable, streakSummary, p

    if len(df) == 0:
        p = P()
        logger.warning(f"No data returned by API for {symbol} by fromAPI")
        return streaksSummaryTable, streakSummary, p

    companyOverview = CompanyOverview(symbol)
    if exchange is None:
        exchange = companyOverview["market_short_name"]

    # closeLastExchange = db.CloseLastExchange(exchange)
    # if not type(closeLastExchange) == datetime.date:
    # closeLastExchange = closeLastExchange.date()

    # closeLastDate = db.CloseLast(symbol, idAsset=idAsset, wise=wise).iloc[0]["date"]
    closeLastDate = db.CloseLastExchange(exchange, wise=wise)
    # fromDate = closeLast.iloc[0]["date"]
    fromDate = df.iloc[-1]["date"]
    streaks = db.ReadStreaksForSymbol(symbol, fromDate=fromDate, wise=wise)
    streaks = streaks.copy()
    streaks.rename(
        columns={
            "streak_len": "streakLen",
            "streak_gain": "streakGain",
            "streak_vol": "streakVolume",
            "start_date": "startDate",
            "end_date": "endDate",
        },
        inplace=True,
    )
    # dateLatestFromDf = df.iloc[0]["date"]
    runningNoneMessage = RunningNoneMessage(streaks, closeLastDate, wise=wise)
    streaks["direction"] = None
    streaks.loc[streaks["streakGain"] < 0.0, "direction"] = "down"
    streaks.loc[streaks["streakGain"] > 0.0, "direction"] = "up"

    if direction is None:
        if len(streaks) > 0:
            # if df.iloc[0]["date"] == streaks.iloc[0]["endDate"]:
            if closeLastDate == streaks.iloc[0]["endDate"]:
                streakRunning = streaks.iloc[0]
        if streakRunning is not None:
            direction = streakRunning["direction"]
        else:
            direction = "up"
        streaks = streaks.loc[streaks["direction"] == direction]
    else:
        streaks = streaks.loc[streaks["direction"] == direction]
        if len(streaks) > 0:
            # if df.iloc[0]["date"] == streaks.iloc[0]["endDate"]:
            if closeLastDate == streaks.iloc[0]["endDate"]:
                streakRunning = streaks.iloc[0]

    ## Adding crash metrics

    streaks = _Crashes(df, streaks, afterStreakDay)
    streaksSummaryTable = StreaksSummaryTable(streaks, streakRunning)

    if streakLen is None:
        if streakRunning is not None:
            streakLen2Disp = streakRunning["streakLen"]
        else:
            streakLen2Disp = None
            # streakLen2Disp = streaks["streakLen"].max()
    else:
        streakLen = int(streakLen)
        streakLen2Disp = streakLen
    streaks2Disp = Streaks2Disp(streaks, streakLen2Disp)
    streakSummary = streaksSummaryTable.loc[
        streaksSummaryTable["streakLen"] == streakLen2Disp
    ]
    streakSummary = streakSummary.copy()

    streakSummary["direction"] = [direction]
    streakSummary["name"] = [companyOverview["name"]]
    streakSummary["sector"] = [companyOverview["sector"]]
    # streakSummary["symbol"] = [companyOverview["Symbol"]]
    streakSummary["symbol"] = [symbol]
    streakSummary["exchange"] = [companyOverview["market_short_name"]]
    streakSummary["currency"] = [companyOverview["currency"]]
    streakSummary["logoUrl"] = [companyOverview["logo_url"]]
    streakSummary["runningNoneMessage"] = [runningNoneMessage]
    streakSummary["dateStamp"] = [closeLastDate]
    streakSummary["afterStreakDay"] = [afterStreakDay]
    streakSummary["wise"] = [wise]
    if len(df) >= 1:
        streakSummary["closePrevDay"] = [df.iloc[0]["close"]]
    else:
        streakSummary["closePrevDay"] = [None]
    if len(df) >= 2:
        streakSummary["closeDiffPrevDay"] = [df["close"].diff(periods=-1).iloc[0]]
        streakSummary["closeDiffPercPrevDay"] = [
            100 * df["close"].diff(periods=-1).iloc[0] / df.iloc[0]["close"]
        ]
    else:
        streakSummary["closeDiffPrevDay"] = [None]
        streakSummary["closeDiffPercPrevDay"] = [None]

    if direction == "down":
        gainMaxMax = streaksSummaryTable["gainMax"].min()
        crashMaxMax = streaksSummaryTable["crashMax"].min()
    else:
        gainMaxMax = streaksSummaryTable["gainMax"].max()
        crashMaxMax = streaksSummaryTable["crashMax"].max()
    streaksSummaryTable["gainMaxBar"] = (
        100.0 * streaksSummaryTable["gainMax"] / gainMaxMax
    )
    streaksSummaryTable["gainMeanBar"] = (
        100.0 * streaksSummaryTable["gainMean"] / gainMaxMax
    )
    streaksSummaryTable["gainRunningBar"] = (
        100.0 * streaksSummaryTable["gainRunning"] / gainMaxMax
    )

    streaksSummaryTable["crashMaxBar"] = (
        100.0 * streaksSummaryTable["crashMax"] / crashMaxMax
    )
    streaksSummaryTable["crashMeanBar"] = (
        100.0 * streaksSummaryTable["crashMean"] / crashMaxMax
    )

    volMaxMax = streaksSummaryTable["volMax"].max()
    streaksSummaryTable["volMaxBar"] = 100.0 * streaksSummaryTable["volMax"] / volMaxMax
    streaksSummaryTable["volMeanBar"] = (
        100.0 * streaksSummaryTable["volMean"] / volMaxMax
    )
    streaksSummaryTable["volRunningBar"] = (
        100.0 * streaksSummaryTable["volRunning"] / volMaxMax
    )

    qVolumeUnit = f"""
        SELECT asset_type.volume_unit
        FROM assets
        JOIN asset_type ON assets.asset_type = asset_type.id
        WHERE assets.id = {idAsset};"""
    volUnit = db.DfFromDb(qVolumeUnit)
    volUnit = volUnit.iloc[0]["volume_unit"]
    streakSummary["volUnit"] = volUnit


    # return streaks2Disp
    p = CompanyWisePlot(df, streaks2Disp)
    return streaksSummaryTable, streakSummary, p


def CompanywiseMomentumAll(symbol, ma=None, periodLen=None, wise="day"):
    """Function returns all the data and plot requ for the companywise momentum page.
    Args:
        symbol (str): symbol of the asset/company/index like GOOG
        ma (int, optional): length of moving aveage window
        periodLen (str, optional): 1M, 3M, 6M,1Y, 5Y, MAX are the valid inputs, if not given, 1M assumed
        wise (str, optional: day/week/month): default day
    Retuns
        momentumsSummaryTable (df): momentum table
        momentumSummary (pd.Series): for the top right summary box
        p (bokeh plot): plot of the momentum
    """
    maRunning = None
    df = None
    momentumMetricsAll = None
    # momentumsSummaryTable = None
    momentumSummary = None

    # periodLens= {"1M":30, "3M":91, "6M":182, "1Y":365, "5Y":(365 * 5) + 1, "MAX":None, "ALL":None}
    # periodLens= {"1M":30, "3M":91, "6M":182, "1Y":366, "2Y":366*2, "3Y":366*3, "5Y":(365 * 5) + 2, "7Y":366*7, "10Y":366*10, "15Y":366*15, "20Y":366*20, "MAX":None, "ALL":None}
    periodLens = {
        "1M": 21,
        "3M": 21 * 3,
        "6M": 21 * 6,
        "1Y": 252,
        "2Y": 252 * 2,
        "3Y": 252 * 3,
        "5Y": 252 * 5,
        "7Y": 252 * 7,
        "10Y": 252 * 10,
        "15Y": 252 * 15,
        "20Y": 252 * 20,
        "MAX": None,
        "ALL": None,
    }
    if periodLen in periodLens:
        periodLen = periodLens[periodLen]
    else:
        periodLen = 252 * 10
    # if periodLen is not None:
    # periodLen200 = periodLen + 201
    #    pass
    # else:
    #    periodLen200 = periodLen

    # df = db.ReadTableDailyData(symbol, limit=periodLen200, idAsset=None)
    df = fromApi.DailyDfFromFmg(symbol, limit=periodLen, wise=wise)

    logger.debug(f"{symbol}, length of df = {len(df)}")
    if not isinstance(df, pd.DataFrame):
        p = P()
        print(f"No data returned by API for {symbol} by fromAPI")
        return momentumMetricsAll, momentumSummary, p

    if len(df) == 0:
        p = P()
        print(f"No data returned by API for {symbol} by fromAPI")
        return momentumMetricsAll, momentumSummary, p

    # fromDate = df.iloc[-1]["date"]
    dateLatestFromDf = df.iloc[0]["date"]
    if not isinstance(dateLatestFromDf, datetime.date):
        dateLatestFromDf = dateLatestFromDf.date()

    momentumMetricsAll = momentum.MomentumMetricsAll(df)

    if ma is None:
        ma = momentumMetricsAll["runLen"][::-1].idxmax()
    else:
        ma = int(ma)
    # momentumSummary = momentumMetricsAll.loc[ma]
    momentumSummary = momentumMetricsAll[momentumMetricsAll.index == ma].copy()
    momentumSummary.loc[:,"ma"] = ma

    companyOverview = CompanyOverview(symbol)

    momentumSummary.loc[:,"name"] = [companyOverview["name"]]
    momentumSummary.loc[:,"sector"] = [companyOverview["sector"]]
    momentumSummary.loc[:,"symbol"] = [symbol]
    momentumSummary.loc[:,"exchange"] = [companyOverview["market_short_name"]]
    momentumSummary.loc[:,"currency"] = [companyOverview["currency"]]
    momentumSummary.loc[:,"logoUrl"] = [companyOverview["logo_url"]]
    momentumSummary.loc[:,"dateStamp"] = [dateLatestFromDf]
    momentumSummary.loc[:,"wise"] = [wise]
    # momentumSummary["afterStreakDay"] = [afterStreakDay]
    if len(df) >= 1:
        momentumSummary.loc[:,"closePrevDay"] = [df.iloc[0]["close"]]
    else:
        momentumSummary.loc[:,"closePrevDay"] = [None]
    if len(df) >= 2:
        momentumSummary.loc[:,"closeDiffPrevDay"] = [df["close"].diff(periods=-1).iloc[0]]
        momentumSummary.loc[:,"closeDiffPercPrevDay"] = [
            100 * df["close"].diff(periods=-1).iloc[0] / df.iloc[0]["close"]
        ]
    else:
        momentumSummary.loc[:,"closeDiffPrevDay"] = [None]
        momentumSummary.loc[:,"closeDiffPercPrevDay"] = [None]

    # p = CompanyWisePlot(df, streaks2Disp)
    df, momentumMetrics = momentum.MomentumMetrics(df, ma=ma)
    p = MomentumPlot(df)

    momentumMetricsAll["ma"] = momentumMetricsAll.index

    return momentumMetricsAll, momentumSummary, p

def CompanywiseSeasonality(symbol, periodLen=None, wise="month"):
    """Function returns all the data no plot required for the companywise seasonality page.
    Args:
        symbol (str): symbol of the asset/company/index like GOOG
        periodLen (str, optional): 1M, 3M, 6M,1Y, 5Y, MAX are the valid inputs, if not given, 1M assumed
        wise (str, optional: month/quarter): default day
    Retuns
        seasonalityDf (df): 

        momentumsSummaryTable (df): momentum table
        momentumSummary (pd.Series): for the top right summary box
        p (bokeh plot): plot of the momentum
    """
    df = None
    seasonalityDf = None
    seasonalityDict = None

    # momentumSummary = None

    periodLens = {
        "1M": 21,
        "3M": 21 * 3,
        "6M": 21 * 6,
        "1Y": 252,
        "2Y": 252 * 2,
        "3Y": 252 * 3,
        "5Y": 252 * 5,
        "7Y": 252 * 7,
        "10Y": 252 * 10,
        "15Y": 252 * 15,
        "20Y": 252 * 20,
        "MAX": None,
        "ALL": None,
    }
    if periodLen in periodLens:
        periodLen = periodLens[periodLen]
    else:
        periodLen = None

    df = fromApi.DailyDfFromFmg(symbol, limit=periodLen, wise=wise)

    logger.debug(f"{symbol}, length of df = {len(df)}")
    if not isinstance(df, pd.DataFrame):
        # p = P()
        print(f"No data returned by API for {symbol} by fromAPI")
        return seasonlaityDf, seasonalityDict

    if len(df) == 0:
        # p = P()
        print(f"No data returned by API for {symbol} by fromAPI")
        return seasonlaityDf, seasonalityDict

    dateLatestFromDf = df.iloc[0]["date"]
    if not isinstance(dateLatestFromDf, datetime.date):
        dateLatestFromDf = dateLatestFromDf.date()

    seasonalityDf, yearsRange = seasonality.SeasonalityDf(df, wise=wise)

    seasonalitySummary = dict()
    # seasonalitySummary["yearsRange"] = yearsRange
    seasonalitySummary["yearStart"] = yearsRange[0]
    seasonalitySummary["yearEnd"] = yearsRange[1]

    companyOverview = CompanyOverview(symbol)

    seasonalitySummary["name"] = [companyOverview["name"]]
    seasonalitySummary["sector"] = [companyOverview["sector"]]
    seasonalitySummary["symbol"] = [symbol]
    seasonalitySummary["exchange"] = [companyOverview["market_short_name"]]
    seasonalitySummary["currency"] = [companyOverview["currency"]]
    seasonalitySummary["logoUrl"] = [companyOverview["logo_url"]]
    seasonalitySummary["dateStamp"] = [dateLatestFromDf]
    seasonalitySummary["wise"] = [wise]

    seasonalitySummary = pd.DataFrame(seasonalitySummary)

    seasonalityDict = dict()
    seasonalityDict[int(yearsRange[0])] = seasonalityDf

    df["date"] = pd.to_datetime(df["date"])
    seasonalityDfs = pd.DataFrame()

    for kYearStart in range(yearsRange[0], yearsRange[1]+1):
        subCount = df["date"].dt.year >= kYearStart
        subCount = subCount.sum() + 1
        dfSub = df.iloc[:subCount]

        seasonalityDf, yearsRangeTrash = seasonality.SeasonalityDf(dfSub, wise=wise)
        seasonalityDf["yearStart"] = kYearStart

        seasonalityDfs = pd.concat([seasonalityDfs, seasonalityDf])

        # seasonalityDict[(kYearStart)] = seasonalityDf
    seasonalityDfs["season"] = seasonalityDfs.index 

    return seasonalityDfs, seasonalitySummary

def CompanywiseYearToMonth(symbol, wise):
    """
    Function to fetch and process stock data for a given symbol.

    Args:
        symbol (str): Stock symbol for which data needs to be fetched.

    Returns:
        pd.DataFrame: Processed DataFrame with year-wise and month-wise gain percentages.
    
    """
    
    companyOverview = CompanyOverview(symbol)
    
    seasonalitySummary = dict()
    seasonalitySummary["name"] = [companyOverview["name"]]
    seasonalitySummary["sector"] = [companyOverview["sector"]]
    seasonalitySummary["symbol"] = [symbol]
    seasonalitySummary["exchange"] = [companyOverview["market_short_name"]]
    seasonalitySummary["currency"] = [companyOverview["currency"]]
    seasonalitySummary["logoUrl"] = [companyOverview["logo_url"]]
    seasonalitySummary["colorMaxMin"] = [[10.0, -10.0]]

    seasonalitySummary = pd.DataFrame(seasonalitySummary)

    try:
        # Simulating the API fetch for stock data
        df = fromApi.DailyDfFromFmg(symbol, limit=None, wise = wise, latestTail = True)
        
        # Validate if data was fetched successfully
        if df is None or df.empty:
            raise ValueError(f"No data found for the symbol: {symbol}")
        
        # Retain only relevant columns and compute gain and gain percentage
        df = df[['date', 'close']].copy()
        df['gain'] = df["close"] - df["close"].shift(-1)
        df['gain_percentage'] = (df["close"] - df["close"].shift(-1)) * 100 / df["close"].shift(-1)
        
        # Convert date to datetime and extract year and month
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if df['date'].isna().any():
            raise ValueError("Invalid dates encountered in the dataset.")
        
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        
        # Reshape the DataFrame to have years as rows and months as columns
        reshapeDf = df.pivot_table(
            index='year',
            columns='month',
            values='gain_percentage'
        )
         

        if wise == "month":
             
            reshapeDf = df.pivot_table(
                index='year',
                columns='month',
                values='gain_percentage'
            )

            reshapeDf.columns = reshapeDf.columns.map(lambda x: pd.Timestamp(f'2024-{x}-01').strftime('%B')[:3]) 

        elif wise == "quarter" :
            # Map month numbers to names and compute yearly total gain percentage
            
            df['quarter'] = df['month'].apply(lambda x: f'Q{((x-1)//3) + 1}')

            reshapeDf = df.pivot_table(
                index='year',
                columns='quarter',
                values='gain_percentage',
                aggfunc='mean'  # In case of multiple rows in a quarter, take the mean
            )
        

        dailyDf = fromApi.DailyDfFromFmg(symbol, limit=None, wise = "day", latestTail = True)
        df_yearToDate = ytd.YearToDateFun(dailyDf)


        #df_yearEnd = df.loc[(df['date'].dt.month == 12) & (df['date'].dt.day == 31), ['year', 'close']].copy()
        df_yearEnd = df[df['date'].dt.month == 12].groupby('year').last().reset_index()[['year', 'close']].sort_values(by = 'year', ascending = False).reset_index(drop = True)
        df_yearEnd['yearEnd'] = ( df_yearEnd['close'] - df_yearEnd['close'].shift(-1))*100/ df_yearEnd['close'].shift(-1)

        reshapeDf = pd.merge(reshapeDf,df_yearToDate[['year','yearToDate']],on = 'year', how = 'left')
        reshapeDf = pd.merge(reshapeDf,df_yearEnd[['year','yearEnd']], on = 'year', how = 'left')

        # Sort the DataFrame by year in descending orderi
        reshapeDf.set_index('year', inplace = True)
        result = reshapeDf.sort_index(ascending = False)        

        seasonalitySummary.iloc[0]["dateStamp"] = [df.iloc[0]["date"]]

        return seasonalitySummary, result

    except ValueError as ve:
        # Handle issues with invalid input or data fetching
        raise ValueError(f"Data processing error: {ve}")

    except Exception as e:
        # Handle unexpected errors during processing
        raise RuntimeError(f"Unexpected error during data processing: {e}")



if __name__ == "__main__":
    """
    # Create a console handler and set the level to DEBUG
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    # Create a formatter and add it to the handler
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)
    logger.debug(f"Logger Level: {logging.getLevelName(logger.getEffectiveLevel())}")
    logger.setLevel(logging.DEBUG)
    logger.debug(f"Logger Level: {logging.getLevelName(logger.getEffectiveLevel())}")
    """
    """
    from bokeh.plotting import show

    symbol = "INFY.NS"
    # symbol = "DFM.AE"
    # symbol = "NBCC.NS"
    streaksSummaryTable, streakSummary, p = CompanywiseDataAll(
        symbol,
        exchange=None,
        direction="up",
        streakLen=3,
        periodLen="6M",
        afterStreakDay=1,
        wise="month",
    )
    """
    pass
    # show(p)
    #    logger.info("CompanywiseDataAll worked")
    #    momentumMetricsAll, df, p = CompanywiseMomentumAll(
    #        symbol, ma=20, periodLen="6M", wise="month"
    #    )
    #    momentumMetricsAll, momentumSummary, p = CompanywiseMomentumAll(
    #        symbol, ma=None, periodLen=None, wise="month"
    #    )
    logger.info("CompanywiseMomentumall worked")
