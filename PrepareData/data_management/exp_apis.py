# pylint: disable=C0301 # disable line too long
# pylint: disable=C0103 # disable snake_case warning

"""
Experiments with different API providers for dubai and saudi markets
"""

import sys
import pandas as pd
import requests

urlStocksList = "https://financialmodelingprep.com/api/v3/stock/list?apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9"

apiKey = "1anPitqreO42ExDOKVBcKpzy6h7ZRtc9"

urlBase = "https://financialmodelingprep.com/api/v3/"

print("before exit")
sys.exit()
print("after exit")

urlStockList = "stock/list"
urlStockList = urlBase + urlStockList + "?" + "apiKey=" + apiKey

ticker = "DFM.AE"
ticker = "AAPL"
urlHistorical = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={apiKey}"

response = requests.get(urlHistorical)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data["historical"])
    tickerFromResponse = data["symbol"]


if "dfTickers" not in locals():
    dfTickers = pd.read_json(urlStocksList)

exchangeShortNames = list(set(dfTickers["exchangeShortName"]))
exchanges = list(set(dfTickers["exchange"]))

"https://financialmodelingprep.com/api/v3/profile/AAPL?apikey=1anPitqreO42ExDOKVBcKpzy6h7ZRtc9"


# DFM
# SAU

# exchangeShortNames = list(set(dfTickers[["exchange", "exchangeShortName"]]))
