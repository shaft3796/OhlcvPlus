# Import OHLCV+, this is the main class.
from ohlcv.ohlcv import OhlcvPlus
# Import ccxt.
import ccxt

"""
We create first our ccxt client, more info at https://github.com/ccxt/ccxt.
While ccxt allows to use a lot of exchanges, it's important to keep in mind that some may have some unexpected 
behavior.
"""
client = ccxt.binance()

"""
We create our OhlcvPlus instance.
The first parameter is the ccxt client instanced above.
The second parameter is optional, OhlcvPlus achieve data persistence through an sqlite database, 
the whole database is stored in a file, this parameter is the path to that file, if the file doesn't exist it will be
created. You can pass None to this parameter do disable data persistence.
"""
ohlcvp = OhlcvPlus(client, database_path='my_data.db')

"""
We are now able to download our first dataframe. The OhlcvPlus class features a download method, but we recommend to 
only use the load method, which is a wrapper around the download method, but with some extra features.
When called for the first time, this method will download the ohlcv, create a field in the database, save it (if the 
database is enabled) and return the dataframe. When called again, the ohlcv will be directly loaded from the database 
and updated with new candles if needed.
The first parameter is the market, e.g 'BTC/USDT'. This parameter is directly passed to CCXT, if you encounter any
error, please refer to the CCXT documentation.
The second parameter is the timeframe, e.g '1m'. This parameter is directly passed to CCXT, if you encounter any
error, please refer to the CCXT documentation.
The third parameter is the date from which you want to download the ohlcv, e.g '2021-01-01 00:00:00'. If no data is 
available for this date, an error will be raised.
The fourth parameter is the limit, this parameter is used to limit the number of candles downloaded. The first possible 
value is an integer indicating the number of candles to download, e.g 1000. The second possible value is a date, e.g
'2021-01-01 00:00:00', in this case, the download will stop when the date is reached. The third possible value is -1,
in this case, the download will stop when there is no more data available.
The fifth parameter is the update parameter, if set to True, the ohlcv will be updated with latest candles when it is 
loaded from the database, this is useful to easily keep up to date data. If you set this parameter to True but the 
limit parameter is set to a date or an integer other than -1, the data will still be updated and saved but the dataframe 
will be returned according to the limit parameter.
The sixth parameter is the verbose parameter, if set to True, the download progress will be displayed.
The seventh parameter is the workers parameter, use this parameter with caution, refer to the download method docstring
For more information about the download method, refer to the download method docstring
"""
# Download 1000 candles from 2023-01-01 00:00:00 to 2023-02-01 00:00:00.
ohlcv1 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=1000, update=True, verbose=True, workers=100)
# Download all candles from 2023-01-01 00:00:00 to 2023-02-01 00:00:00.
ohlcv2 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit='2023-02-01 00:00:00', update=True, verbose=True, workers=100)
# Download all candles from 2023-01-01 00:00:00 to now.
ohlcv3 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=-1, update=True, verbose=True, workers=100)

# Same as the first request, but this one the ohlcv will be loaded and updated from the database.
ohlcv4 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=1000, update=True, verbose=True, workers=100)