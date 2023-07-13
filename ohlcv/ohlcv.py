import datetime
import math
import multiprocessing
import threading
import time
import sqlalchemy as orm
from colorama import Fore

from ohlcv.utils import Bar, date_to_timestamp, timestamp_to_date, generate_sign
import ccxt
import pandas as pd

LIMIT = 1000000


class NotEnoughDataException(Exception):
    pass


class Request:

    def __init__(self, market, timeframe, since, limit):
        self.market = market
        self.timeframe = timeframe
        self.since = since
        self.limit = limit


class OhlcvPlus:

    def __init__(self, client: ccxt.Exchange, database_path: str = "ohlcvplus.db"):
        """
        Initialize the main class.
        :param client: an initialized ccxt client e.g. ccxt.binance()
        :param database_path: Persistence is achieved through sqlite3. This parameter is the path to the database
        file. If None, the persistence will be disabled.
        """
        self.client = client
        self.db = orm.create_engine(f'sqlite:///{database_path}', echo=False, future=True) if database_path else None
        self.conn = self.db.connect() if self.db else None
        self.metadata = orm.MetaData() if self.db else None
        if self.db is not None:
            # --- Main Table ---
            self.table = orm.Table('ohlcv', self.metadata,
                                   orm.Column('signature', orm.String, primary_key=True),
                                   orm.Column('exchange', orm.String),
                                   orm.Column('market', orm.String),
                                   orm.Column('timeframe', orm.String),
                                   orm.Column('since', orm.BigInteger),
                                   orm.Column('limit', orm.BigInteger))
            self.metadata.create_all(self.db)
            self.conn.commit()

            # --- Ohlcv Tables ---
            # Load all existing tables
            signatures = self.conn.execute(orm.select(self.table.c.signature)).fetchall()
            self.tables = {signature: orm.Table(signature, self.metadata, autoload_with=self.db)
                           for signature in [s[0] for s in signatures]}

    def _fetch_ohlcv(self, request):
        """
        Fetch OHLCV data from the exchange.
        :param request: a Request object
        :return: a list of OHLCV data
        """
        df = pd.DataFrame(self.client.fetch_ohlcv(request.market, request.timeframe, request.since, request.limit),
                          columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        for i in range(len(df)):
            df.at[i, 'timestamp'] = int(df.at[i, 'timestamp'])
        return df

    def download(self, market: str, timeframe: str, since: str, limit: (int, str), verbose: bool = True,
                 workers: int = 100):
        """
        Download an Ohlcv dataset from the exchange.
        :param market: the market as a string e.g. 'BTC/USDT', this market must be available on the exchange.
        :param timeframe: the timeframe as a string e.g. '1m', this timeframe must be available on the exchange.
        :param since: the start date as a string e.g. '2020-01-01 00:00:00'
        :param limit: the number of candles to download as an integer e.g. 1000, -1 to download all available data or
        a date as a string e.g. '2020-02-01 00:00:00'.
        :param verbose: whether to print the progress bar or not.
        :param workers: the number of threads to use for downloading. A high number cand lead to several issues such
        as missing data and exchange bans. Use with caution. If you encounter issues, try reducing this number,
        default is 100.

        :return: a pandas DataFrame containing the downloaded data with the following columns: "timestamp", "open",
        "high", "low", "close", "volume". Warning, the timestamp is in milliseconds.
        :raise Exception: Any exception raised by the ccxt library will be raised by this method. This method will also
        raise an exception if no data is available for the requested market and timeframe.
        """
        # --- Pre Download ---
        since = date_to_timestamp(since)
        ohlcv = self._fetch_ohlcv(Request(market, timeframe, since, LIMIT))
        if ohlcv.empty:
            raise NotEnoughDataException("No data available for the requested market and timeframe.")
        if len(ohlcv) == 1:
            raise NotEnoughDataException("Not enough data available for the requested market and timeframe.")
        max_limit = len(ohlcv)
        tf = (ohlcv['timestamp'].iloc[1] - ohlcv['timestamp'].iloc[0])
        # Parse the limit to an integer
        if limit == -1:
            limit = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(limit, str):
            limit = math.ceil((date_to_timestamp(limit) - ohlcv['timestamp'].iloc[0]) / tf)

        # --- Scheduling ---
        len_requests = math.ceil(limit / max_limit)
        requests = []
        for i in range(len_requests):
            requests.append(Request(market, timeframe, since + i * max_limit * tf, LIMIT))
        # load balancing
        jobs = [[] for _ in range(workers)]
        for i in range(len_requests):
            jobs[i % workers].append(i)

        # Set up the progress bar
        from_date = timestamp_to_date(ohlcv['timestamp'].iloc[0])
        to_date = timestamp_to_date(ohlcv['timestamp'].iloc[0] + (limit - 1) * tf)
        bar = None
        if verbose:
            print(Fore.CYAN, f"Downloading {market} {timeframe} data from {from_date} to {to_date}", Fore.RESET)
            bar = Bar(len_requests)

        # Placeholders for the data
        responses = {i: [None, multiprocessing.Semaphore(0)] for i in range(len_requests)}
        executed = 0

        # Placeholder for monitoring
        rate_limited = False

        # --- Download ---
        def exec_requests(indexes):
            nonlocal executed, rate_limited
            for k in range(len(indexes)):
                idx = indexes[k]
                while True:
                    while rate_limited:
                        time.sleep(1)
                    try:
                        responses[idx][0] = self._fetch_ohlcv(requests[idx])
                        responses[idx][1].release()
                        executed += 1
                        break
                    except Exception as e:
                        rate_limited = True

        def monitoring():
            nonlocal executed, rate_limited

            last_executed = 0
            while executed < len_requests:
                if last_executed != executed:
                    last_executed = executed
                if rate_limited:
                    timer = 60
                    while timer > 0:
                        bar.update(executed, front=f"Rate limit reached, waiting for {timer} seconds")
                        time.sleep(1)
                        timer -= 1
                    rate_limited = False

                if verbose:
                    bar.update(executed)
                time.sleep(0.1)

        # Start monitoring
        mt = threading.Thread(target=monitoring)
        mt.start()

        # Start downloading
        threads = [threading.Thread(target=exec_requests, args=(jobs[i],)) for i in range(len(jobs))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        mt.join()
        bar.finish()

        if verbose:
            print(Fore.CYAN, f"Aggregating {len(requests)} dataframe for a total of {limit} candles. This might take "
                             f"some time.", Fore.RESET)
        df = pd.concat([responses[i][0] for i in range(len(requests))], ignore_index=True).iloc[:-1].iloc[:limit]
        df = df.drop_duplicates(subset=['timestamp'])
        df = df.reset_index(drop=True)
        if verbose:
            print(Fore.CYAN, f"Verifying data integrity.", Fore.RESET)
        ic = 0
        for i in range(1, len(df)):
            if df['timestamp'].iloc[i] - df['timestamp'].iloc[i - 1] != tf:
                # Check the number of missing candles
                missing = int((df['timestamp'].iloc[i] - df['timestamp'].iloc[i - 1]) / tf)
                ic += missing

        if ic > 0 and verbose:
            msg = f"WARNING: Integrity check failed:\n  {ic} candles were detected as missing, this is most likely " \
                  f"not a download issue but rather an exchange issue, some exchanges do not provide data for " \
                  f"downtimes or other reasons."
            print(Fore.YELLOW, msg, Fore.RESET)
        return df

    def update(self, dataframe: pd.DataFrame, market: str, timeframe: str, verbose: bool = True, workers: int = 100):
        """
        Update an existing dataframe with new data.
        :param dataframe: the dataframe to update.
        :param market: the market as a string e.g. 'BTC/USDT', this market must be available on the exchange.
        :param timeframe: the timeframe as a string e.g. '1m', this timeframe must be available on the exchange.
        :param verbose: whether to print the progress bar or not.
        :param workers: the number of threads to use for downloading. A high number cand lead to several issues such
        as missing data and exchange bans. Use with caution. If you encounter issues, try reducing this number,
        default is 100.
        """
        since = timestamp_to_date(dataframe['timestamp'].iloc[-1])
        limit = -1
        if verbose:
            print(Fore.CYAN, f"Updating {market} {timeframe} data from {since}", Fore.RESET)
        try:
            new_data = self.download(market, timeframe, since, limit, verbose, workers)
        except NotEnoughDataException:
            return dataframe
        df = pd.concat([dataframe, new_data], ignore_index=True)
        df = df.drop_duplicates(subset=['timestamp'])
        df = df.reset_index(drop=True)
        return df

    def load(self, market: str, timeframe: str, since: str, limit: (int, str), update: bool = False,
             verbose: bool = True, workers: int = 100):
        """
        Load an ohlcv. If you initialized this class with None as 'database_path' parameter, this method will download
        the data. Otherwise, it will load the data from the database. If the database does not contain the data, it
        will download it and save it to the database.
        :param market: the market as a string e.g. 'BTC/USDT', this market must be available on the exchange.
        :param timeframe: the timeframe as a string e.g. '1m', this timeframe must be available on the exchange.
        :param since: the starting date as a string e.g. '2021-01-01 00:00:00'.
        :param limit: the number of candles to download, if -1, all available candles will be downloaded.
        :param update: whether to update the data or not.
        :param verbose: whether to print the progress bar or not.
        :param workers: the number of threads to use for downloading. A high number cand lead to several issues such
        as missing data and exchange bans. Use with caution. If you encounter issues, try reducing this number,
        default is 100.

        :return: a pandas dataframe containing the ohlcv.
        :raise Exception: Raise any exception that might occur during the download.
        """
        if self.db is None:
            return self.download(market, timeframe, since, limit, verbose, workers)
        else:
            signature = generate_sign(market, timeframe, since)
            if signature not in self.tables:
                # Download the data
                df = self.download(market, timeframe, since, limit, verbose, workers)
            else:
                if verbose:
                    print(Fore.CYAN, f"Dataframe found in the database, loading it.", Fore.RESET)
                # Load the data
                t = self.tables[signature]
                # Select timestamp open high low close volume
                data = self.conn.execute(t.select()).fetchall()
                df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                if update:
                    df = self.update(df, market, timeframe, verbose, workers)

            if signature not in self.tables or update:
                if signature not in self.tables:
                    # Create the table
                    t = orm.Table(signature, self.metadata,
                                  orm.Column('timestamp', orm.BigInteger, primary_key=True),
                                  orm.Column('open', orm.Float),
                                  orm.Column('high', orm.Float),
                                  orm.Column('low', orm.Float),
                                  orm.Column('close', orm.Float),
                                  orm.Column('volume', orm.Float))
                    self.metadata.create_all(self.db)
                    # Add the table to the main table
                    self.tables[signature] = t
                    _since = int(df['timestamp'].iloc[0])
                    _to = int(df['timestamp'].iloc[-1])
                    self.conn.execute(
                        self.table.insert().values(signature=signature, exchange=self.client.name,
                                                   market=market, timeframe=timeframe,
                                                   since=_since, limit=_to))
                    self.conn.commit()

                # Save the data
                t = self.tables[signature]
                print(Fore.CYAN, f"Saving {market} {timeframe} data to the database.", Fore.RESET)
                # Update the database table (t) will all new data
                # Check if the database is empty
                if self.conn.execute(t.select()).fetchone() is None:
                    # Insert all the data
                    self.conn.execute(t.insert(),
                                      [{'timestamp': row['timestamp'], 'open': row['open'], 'high': row['high'],
                                        'low': row['low'], 'close': row['close'], 'volume': row['volume']} for
                                       _, row in df.iterrows()])
                else:
                    to = self.conn.execute(self.table.select().where(self.table.c.signature == signature)).fetchone()[5]
                    data = [{'timestamp': row['timestamp'], 'open': row['open'], 'high': row['high'],
                                'low': row['low'], 'close': row['close'], 'volume': row['volume']} for _, row in
                                df.iterrows() if row['timestamp'] > to]
                    if len(data) > 0:
                        self.conn.execute(t.insert(), data)
                self.conn.commit()

                # Update the main table
                self.conn.execute(self.table.update().where(self.table.c.signature == signature).values(
                    limit=int(df['timestamp'].iloc[-1])))
                self.conn.commit()
            # truncate the dataframe according to the limit
            tf = (df['timestamp'].iloc[1] - df['timestamp'].iloc[0])
            # Parse the limit to an integer
            if limit == -1:
                limit = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(limit, str):
                limit = math.ceil((date_to_timestamp(limit) - df['timestamp'].iloc[0]) / tf)
            df.truncate(after=limit-1)
            return df