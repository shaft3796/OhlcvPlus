"""
MIT License

Copyright (c) 2022 Shaft

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import os
import threading
import time
from datetime import datetime
from types import NoneType
import pandas as pd


# Utils ----------------------------------------------------------------------------------------------------------------
class Colors:
    """
    Terminal colors
    """
    LIGHT_YELLOW = '\33[91m'
    LIGHT_GREEN = '\33[93m'
    LIGHT_BLUE = '\33[94m'
    LIGHT_PURPLE = '\33[95m'
    LIGHT_CYAN = '\33[96m'
    LIGHT_WHITE = '\33[97m'
    RED = '\33[31m'
    YELLOW = '\33[33m'
    GREEN = '\33[32m'
    BLUE = '\33[34m'
    PURPLE = '\33[35m'
    CYAN = '\33[36m'
    WHITE = '\33[37m'
    END = '\33[0m'
    ERROR = '\x1b[0;30;41m'


def progress_bar(curent: (int, float), total: (int, float), bar_length: int = 100, front: str = ""):
    """
    Prints a progress bar.
    :param curent: Current value.
    :param total: Number of total values.
    :param bar_length: Length of the progress bar.
    :param front: Text to be printed before the progress bar.
    """

    progress = round(curent / total * 100, 2)
    filled = round((progress / 100 * bar_length)) * "█"
    unfilled = round((bar_length - progress / 100 * bar_length)) * "░"

    print(f"\r{front}{Colors.GREEN}[{filled + unfilled}] {Colors.YELLOW}{progress}% [{curent}/{total}]", end="")
    if progress >= 100:
        print("")


def only_implemented_types(func):
    """
    Decorator to raise an exception when a not implemented type is given as a parameter to a function
    :param func: function to be decorated
    :return: decorated function
    """
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        parameters = func.__code__.co_varnames
        annotations = func.__annotations__
        arguments = {}
        # making arguments dictionary to put them with their parameter name
        for i in range(len(args)):
            arguments[parameters[i]] = args[i]
        for parameter in kwargs:
            arguments[parameter] = kwargs[parameter]

        # apply verification to all parameters in annotations
        for parameter in annotations:
            if parameter not in arguments:
                continue
            if not isinstance(arguments[parameter], annotations[parameter]):
                raise TypeNotImplemented(parameter, annotations[parameter], type(arguments[parameter]))

        return func(*args, **kwargs)

    return wrapper


class TypeNotImplemented(BaseException):
    """
    Exception to be raised when a not implemented type is given to a function
    """

    def __init__(self, parameter_name, required_type, given_type):
        """
        Constructor
        :param parameter_name: name of the parameter involved in the exception
        :param required_type:
        """
        super().__init__(
            f"{Colors.ERROR}TypeNotImplemented exception {given_type} is not implemented for this parameter, parameter "
            f"{parameter_name} must be {required_type}{Colors.END}")


# ----------------------------------------------------------------------------------------------------------------------

# Engine
class OhlcvRequest:

    @staticmethod
    def __date_to_timestamp__(date: str) -> int:
        """
        :param date: "%d/%m/%y %H:%M:%S"
        :return: timestamp
        """
        return int(datetime.timestamp(datetime.strptime(date, '%d/%m/%Y %H:%M:%S'))) * 1000

    def generate_file_name(self):
        filename = self.market.replace("-", "").replace("/", "") + '_'  # market
        filename += self.timeframe + '_'  # timeframe
        filename += datetime.fromtimestamp(self.since / 1000).strftime("%d-%m-%y-%H-%M-%S") + '_'  # since
        filename += ("to_now" if self.limit == -1 else str(self.limit) + '_candles') + '.csv'  # limit

    def __init__(self, market: str, timeframe: str, since: (str, int), limit: int, convert_since: bool = False):
        self.market = market
        self.timeframe = timeframe
        self.since = self.__date_to_timestamp__(since) if convert_since else since
        self.limit = int(limit)


class OhlcvPlus:

    def __init__(self, client):
        self.client = client

    def __fetch_ohlcv__(self, request: OhlcvRequest):
        ohlcv = pd.DataFrame(self.client.fetch_ohlcv(request.market, request.timeframe, request.since, request.limit))
        if ohlcv.empty:
            return ohlcv
        else:
            ohlcv.rename(columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'},
                         inplace=True)
            return ohlcv

    def __download__(self, request: OhlcvRequest, output: bool = True, max_workers: int = 10):
        # 1-) First set of candles
        remaining_candles = request.limit  # Max number of candles to being downloaded
        try:
            ohlcv = self.__fetch_ohlcv__(request)
        except Exception as e:
            print(f"{Colors.ERROR}[OHLCV+] Error while downloading {request.market} {request.timeframe} since "
                  f"{request.since}, download canceled{Colors.END}")
            print(e)
            return pd.DataFrame()
        if ohlcv.empty:  # Return if df is empty
            return ohlcv
        ohlcv_size = len(ohlcv)
        remaining_candles -= ohlcv_size
        if remaining_candles <= 0 or len(ohlcv) < 2:  # Return if there are no more candles to download
            return ohlcv

        # 2-) Requests planning
        genesis_timestamp = ohlcv.iloc[0]["timestamp"]  # First timestamp
        second_timestamp = ohlcv.iloc[1]["timestamp"]  # Second timestamp
        last_timestamp = ohlcv.iloc[-1]["timestamp"]  # Last timestamp
        rows_offset = second_timestamp - genesis_timestamp  # time between two df rows
        request_offset = last_timestamp - genesis_timestamp  # time between first & last df rows
        requests = []
        rid = 0  # request ID
        while remaining_candles > 0:
            last_timestamp += rows_offset
            # Check to not download the future -----
            if last_timestamp > time.time() * 1000:
                remaining_candles = 0
                break
            # -------------------------------------
            requests.append((rid, last_timestamp, remaining_candles))
            remaining_candles -= ohlcv_size
            last_timestamp += request_offset
            rid += 1
        if len(requests) == 0:
            return ohlcv
        # We divide requests between workers
        curent_worker = 0
        workers_jobs = []
        for r in requests:
            if len(workers_jobs) < curent_worker + 1:
                workers_jobs.append([])
            workers_jobs[curent_worker].append(r)
            curent_worker += 1
            if curent_worker >= max_workers:
                curent_worker = 0

        # 3-) Requests execution
        cache = {}  # Containing df to being concatenated
        rate_limit_reached = False  # flag set by dl threads and checked by rate limit thread
        finished = False  # flag used to stop rate limiter thread
        request_id = 0  # Last response

        # Rate limiter
        def rate_limiter():
            nonlocal rate_limit_reached, finished
            remaining_sleep = 61  # time in seconds to wait before next request
            while True:
                if rate_limit_reached:
                    while remaining_sleep > 0:
                        progress_bar(request_id, len(requests), front=f" rate limit reached waiting for "
                                                                      f"[{remaining_sleep}s]") if output else None
                        time.sleep(1)
                        remaining_sleep -= 1
                    remaining_sleep = 61  # Reset
                    rate_limit_reached = False
                if finished:
                    break
                time.sleep(0.1)

        rlt = threading.Thread(target=rate_limiter)  # Starting rate limiter thread
        rlt.start()

        # Exec request
        def exec_request(worker_jobs):
            nonlocal rate_limit_reached, cache, request_id
            for job in worker_jobs:
                _request = OhlcvRequest(request.market, request.timeframe, int(job[1]), job[2])
                while True:
                    try:
                        response = self.__fetch_ohlcv__(_request)
                        break
                    except Exception as e:
                        rate_limit_reached = True
                        while rate_limit_reached:
                            time.sleep(1)

                cache[job[0]] = response
                request_id = job[0] if job[0] > request_id else request_id

        print(f"{Colors.YELLOW}[OHLCV+] Multithreading Download" if output else None)
        progress_bar(0, len(requests))

        # Progress Bar
        def progress_bar_thread():
            while True:
                progress_bar(request_id, len(requests)) if not rate_limit_reached else None
                if finished:
                    break
                time.sleep(1)

        pbt = threading.Thread(target=progress_bar_thread)
        pbt.start()

        # Schedule requests
        threads = []
        for jobs in workers_jobs:
            threads.append(threading.Thread(target=exec_request, args=(jobs,)))
            threads[-1].start()
        for thread in threads:
            thread.join()

        finished = True
        pbt.join()
        rlt.join()

        # 4-) Concatenation
        print(f"\n{Colors.YELLOW}[OHLCV+] Concatenating dataframes" if output else None)
        ohlcv = [ohlcv]
        for i in range(len(cache)):
            ohlcv.append(cache[i])

        ohlcv = pd.concat(ohlcv, ignore_index=True)

        # We are here waiting for the rate limit thread to stop
        return ohlcv

    @staticmethod
    def __generate_filename__(request: OhlcvRequest):
        return f"{request.market.replace('-', '').replace('/', '')}_{request.timeframe}_" \
               f"{datetime.fromtimestamp(request.since/1000).strftime('%d-%m-%y-%H-%M-%S')}_{request.limit}.csv"

    @only_implemented_types
    def load_ohlcv(self, market: str, timeframe: str, since: str, limit: int, output: bool = True,
                   max_workers: int = 100, path: (str, NoneType) = "data/") -> pd.DataFrame:
        """
        [Doc]
        Load ohlcv method work as the get_kline method with some more features:
        you can very quickly download a lot of candles using multithreading with just one call.
        you can save your data as a csv file to be able to load it from a file when you need it to gain a lot of speed.
        you can pass -1 wich indirectely mean "now" as the limit to update your data everytime you load it with
        the last candles, the method just download candles you don't have.
        [Params]
        :param market: example "BTC/USD".
        :param timeframe: usually '1y', '1m', '1d', '1w', '1h'...
        :param since: date of the first candle to download, format: "day-month-year hours:minuts:seconds"
        e.g: 17-10-2022 "22:39:41"
        :param limit: number of candles you want to download, use -1 to download as many candles as possible and to
        update your dataframe with the last candles if you chose to save your data to the filesystem.
        :param output: Display download and load information, nottably the progress bar.
        :param max_workers: number of requests sheduled at the same time, increase this parameter may cause some
        issues ! Decrease this parameter will make the download slower, but you can do it if you encounter some issues.
        :param path: None will disable the data saving to the file system and everytime you call this method,
        data will be downloaded. If you pass a path as a string to this parameter or let the default value, data will
        be saved to this path as csv files and the method will check to this path if a file exists with data you want
        to load.
        [Returns]
        :return: a pandas dataframe indexed from 0 to your number of candles minus one with these columns :
        'timestamp' | 'open' | 'high' | 'low' | 'close' | 'volume'
        """

        def parse_since(_since: str):
            return int(datetime.timestamp(datetime.strptime(_since, '%d/%m/%Y %H:%M:%S')))

        since = parse_since(since) * 1000

        # mkdir if path doesn't exist
        if path is not None:
            if not os.path.exists(path):
                os.mkdir(path)

        def append(df):
            """
            This method update a dataframe with new candles
            """
            print(f"{Colors.PURPLE} OHLCV+ is updating your data with new candles {Colors.END}")
            # time between 2 rows + last timestamp
            _since = int(df.iloc[1]["timestamp"] - df.iloc[0]["timestamp"] + df.iloc[-1]["timestamp"])
            _limit = int((time.time() * 1000 - _since) / 60000 + 100)
            _request = OhlcvRequest(request.market, request.timeframe, _since, _limit)
            _df = self.__download__(_request, output=output, max_workers=max_workers)
            df = pd.concat([df, _df], ignore_index=True)
            return df

        request = OhlcvRequest(market, timeframe, since, limit)
        filename = self.__generate_filename__(request)
        if request.limit == -1:
            request.limit = 1000000000000

        # Check if user want to enable file system
        if path is None:
            # Case 1 - saving to file system disable
            return self.__download__(request, output, max_workers)

            pass
        else:
            # File system enabled, check if our data was already saved in a file
            fullpath = path + filename
            if os.path.exists(fullpath):
                # Case 2 - File system enable, data was already downloaded, we load it
                dataframe = pd.read_csv(fullpath, index_col=0)
                # Check if we have to update the dataframe
                dataframe = append(dataframe)
                # We save the new dataframe
                dataframe.to_csv(path + filename)

                return dataframe
            else:
                # Case 3 - We download & save market data
                dataframe = self.__download__(request, output, max_workers)
                # We save the new dataframe
                dataframe.to_csv(path + filename)

                return dataframe
