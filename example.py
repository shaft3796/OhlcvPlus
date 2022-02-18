# Import OHLCV+
from ohlcv import OhlcvPlus
# Import CCXT
import ccxt

# To make ohlcv+ working, you have to provide a ccxt instanced client, if you already have one, skip this step.
client = ccxt.binance()  # Replace binance by any exchange supported by CCXT

# OHLCV+ engine instanciation
ohlcv_engine = OhlcvPlus(client)  # Provide your ccxt instanced client

# As mentionned in the feature section, everything is does by using one method: OhlcvPlus.load_ohlcv(...).
# Let's understand how this method works:
#
# First case, path parameter is unfilled (default value is data/) or is filled with a path
# If a file exists with your data, the manager will load it,
# if there's new candles targeted by the time range you set, the manager will update your data
# If there is no file with your data, the manager will download your data and save it as a CSV file
#
# Second case, path parameter is filled with None
# The manager will just download the data from the exchange
#
# Now let's see others parameters:
#
# - market: a string with the market you want to download the data from e.g. "BTC/USDT" - timeframe: timeframe of
# your ohlcv, usually '1y', '1m', '1d', '1w', '1h', '5M'.. depending of the exchange used - since: the date of the
# first candle of your OHLCV as a string with this format: "day-month-year hours:minuts:seconds" e.g: 17/10/2022
# "22:39:41"
# - limit: the number of candles to download, if you pass -1 the manager will download as many candles as
# possible Optionals parameters: - output: True to display information for example the progress bar, False else.
# - max_workers: number of requests sheduled at the same time, increase this parameter may cause some issues ! Decrease
# this parameter will make the download slower, but you can do it if you encounter some issues. - path: Path of the
# directory where you want your data to be located, pass None to disable the filesystem
#
# This method returns a pandas dataframe indexed from 0 to n (your number of candles minus one)
# The pandas dataframe has these columns: 'timestamp' | 'open' | 'high' | 'low' | 'close' | 'volume'

# Let's see an example

# We use FTX
client = ccxt.ftx()
ohlcv_engine = OhlcvPlus(client)

# We download OHLCV of BTC/USD with one candle every one day from 01/01/2020 00:00:00, and we download as many candles
# as possible, we save the data in a file in ohlcv/ directory
data = ohlcv_engine.load_ohlcv("BTC/USD", "1m", "01/01/2020 00:00:00", -1, output=True, path="ohlcv/")
print(data)

# The manager found a file with our data, it loads it and update it with new candles, this was faster than the previous
# method
data = ohlcv_engine.load_ohlcv("BTC/USD", "1m", "01/01/2020 00:00:00", -1, output=True, path="ohlcv/")
print(data)
