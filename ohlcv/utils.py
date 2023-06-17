import datetime
import hashlib
import sys
import time

from colorama import Fore, Back, Style

BOLD = "\033[1m"


class Bar:
    def __init__(self, max_value: (int, float), bar_len: int = 100, front: str = "", back: str = ""):
        self.max_value = max_value
        self.bar_len = bar_len
        self.front = front
        self.back = back
        self.current = 0
        self.bar_anim = ['+', '-']
        self.bar_anim_state = 0
        self.print_bar()

    def print_bar(self, front="", back=""):
        progress = self.current / self.max_value
        filled = int(progress * self.bar_len)
        empty = self.bar_len - filled
        ac = self.bar_anim[self.bar_anim_state]
        self.bar_anim_state += 1 if self.bar_anim_state < len(self.bar_anim) - 1 else -self.bar_anim_state
        s = f"{Fore.CYAN}{self.front} {front} {round(progress * 100, 1)}% {Fore.MAGENTA + BOLD}[{'█' * filled}{'░' * empty}] [{ac}] {Style.RESET_ALL + Fore.CYAN} {back} {self.back}{Style.RESET_ALL}"
        print('\r' + s, end="")

    def update(self, current: (int, float), front="", back=""):
        self.current = current
        self.print_bar(front, back)

    def finish(self):
        self.update(self.max_value)
        print()


def date_to_timestamp(date):
    return int(datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


def timestamp_to_date(timestamp):
    timestamp = int(timestamp)
    return datetime.datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')


def generate_sign(exchange, market, timeframe):
    return hashlib.md5(f"{exchange}{market}{timeframe}".encode()).hexdigest()[0:16]
