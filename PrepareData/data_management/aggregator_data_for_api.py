"""
aggregator_data_for_api.py
This module has the functions required for the aggregator page APIs.
This function calls the methods from streks.py and db.py to create data required.

"""

# pylint: disable=C0301
# Your long line of code goes here
# pylint: disable=C0103
# Disbling sname_case warning
import numpy as np

if __name__ in ["__main__", "aggregator_data_for_api"]:
    import streaks as streakLib
    import db
else:
    from . import streaks as streakLib
    from . import db

# import streaks as streakLib
# import db as db

import logging

logger = logging.getLogger(__name__)
logger.info("Starting data_update.py")


def BarChart(direction="", exchange=None, wise="day"):
    """
    This method returns a dictionary of countOfStreaks against streakLengths

    """
    if exchange is None:
        logger.error("HistStreaksRunning exchange is None")
        return
    if direction in ["up", "down"]:
        return streakLib.HistStreaksRunning(direction, exchange, wise=wise)
    logger.error(
        "Check aggregator_data_for_api, BarChart. direction should be up or down"
    )
    return None


def StreaksRunningSummary(direction="", exchange="NASDAQ", market_id=None, wise="day"):
    """
    This method returns a dictionary, which corresponds to the table to be displyed as streaks summary in the aggregator page.
    The input arguments are up/down and it's compulsory
    """
    # streaksRunningSummary = db.ReadTable("streaks_running_summary")
    if direction == "up":
        compare = ">"
    elif direction == "down":
        compare = "<"
    else:
        return None

    if wise is None:
        wise = "day"

    queryStreaksRunningSummary = f"SELECT srs.*, market.market_short_name, \
            assets.market_cap \
            FROM streaks_running_summary srs \
            JOIN assets ON srs.symbol_id = assets.id \
            JOIN market ON assets.market = market.market_id \
            WHERE market.market_short_name = '{exchange}' \
            AND srs.gain_running {compare} 0 \
            AND srs.wise = '{wise}' \
            ;"
    streaksRunningSummary = db.DfFromDb(queryStreaksRunningSummary)

    streaksRunningSummary = streaksRunningSummary[
        [
            "symbol",
            "name_asset",
            "streak_len",
            "occurrences_count",
            "gain_max",
            "gain_mean",
            "gain_running",
            "vol_median",
            "streak_df_close",
            "symbol_id",
            "market_short_name",
            "market_cap",
            "wise",
        ]
    ]
    #    if direction == "up":
    #        barDivider = streaksRunningSummary["gain_max"].max() / 100.0
    #    elif direction == "down":
    #        barDivider = streaksRunningSummary["gain_max"].min() / 100.0
    #    else:
    #        print("Error")
    # streaksRunningSummary["bar_gain_max"] = streaksRunningSummary["gain_max"] / barDivider
    streaksRunningSummary["bar_gain_max"] = 100.0
    # streaksRunningSummary["bar_gain_mean"] = streaksRunningSummary["gain_mean"] / barDivider
    streaksRunningSummary["bar_gain_mean"] = (
        100.0 * streaksRunningSummary["gain_mean"] / streaksRunningSummary["gain_max"]
    )
    # streaksRunningSummary["bar_gain_running"] = streaksRunningSummary["gain_running"] / barDivider
    streaksRunningSummary["bar_gain_running"] = (
        100.0
        * streaksRunningSummary["gain_running"]
        / streaksRunningSummary["gain_max"]
    )

    def ScaledCloseForPlot(c):
        # print(c)
        if c is not None:
            c = np.array(c)
            c = c - c.min()
            c = 100.0 * c / c.max()
            c = c[::-1]
            c = c.tolist()
        return c

    streaksRunningSummary["chart_streak_df_close"] = streaksRunningSummary[
        "streak_df_close"
    ].apply(ScaledCloseForPlot)
    return streaksRunningSummary


def Exchanges():
    """Returns the list of available markets for the aggregator page
    Think of adding indexes in future
    """
    exchanges = db.ReadTable("market", by="market_id")
    return exchanges


def LastAggregatorUpdate(exchange=None, wise="day"):
    # logger.debug(f"{exchange} calling LastAggregatorUpdate")
    if not isinstance(exchange, str):
        logger.debug(f"{exchange} in not of type str")
        return None
    # q = f"SELECT last_aggregator_update FROM market WHERE market_short_name = '{exchange}';"
    q = f"SELECT date FROM last_aggregator_update JOIN market ON last_aggregator_update.market_id = market.market_id WHERE market.market_short_name = '{exchange}' AND wise = '{wise}';"
    lastAggregatorUpdate = db.DfFromDb(q)
    # print("lastAggregatorUpdate:", lastAggregatorUpdate)
    if len(lastAggregatorUpdate) == 1:
        lastAggregatorUpdate = lastAggregatorUpdate.iloc[0]["date"]
        logger.debug(
            f"{exchange} retuning lastAggregator update {lastAggregatorUpdate}"
        )
        return lastAggregatorUpdate
    logger.warning("last_aggregator_update not returned")
    return None
