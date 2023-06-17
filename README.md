# OHLCV+

<img heigh=auto width=15% src="https://github.com/Shaft-3796/Shaft/blob/main/OHLCV+.png">

Download market data, aka OHLCV for Open High Low Close Volume, is something very useful for a trader.
OHLCV+ provide you a way to manage your data.

## Features 📋

- Download market data very quickly using multithreading (in the respect of API rate limit).
- Save your data in a lightweight database.
- Load your data from the database very quickly.
- Update your data to keep up-to-date ohlcvs.
- Super simple with only one method to use.

## Tutorial 🔎

#### Dependencies

- Pandas
- CCXT
- colorama
- SQLAlchemy

#### Installation

The package is packaged as a pip package, you can install it with this command:
```pip install OhlcvPlus```

You can also manually download this project and its dependencies.
```pip install -r requirements.txt```

#### Usage

OhlcvPlus is super easy to use !
To see how it works, just look at the example.py file.
The code is also commented, you can easily take a look at the methods docstrings.

```python
#Import OHLCV+, this is the main class.
#Import ccxt.

client = ccxt.binance()
ohlcvp = OhlcvPlus(client, database_path='my_data.db')
ohlcv1 = ohlcvp.load(market='BTC/USDT', timeframe='1m', since='2023-01-01 00:00:00', limit=1000, update=True, verbose=True, workers=100)
```