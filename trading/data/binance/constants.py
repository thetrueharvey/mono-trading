"""Binance API constants
"""
# stdlib
from enum import Enum

BASE_URL = f"""https://api.binance.com/api/v3"""
SYMBOL_INFO_JSON_FIELDS = ["orderTypes", "filters", "permissions"]

KLINE_FIELDS = [
    "Open time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close time"
]
KLINE_FIELDS_START, KLINE_FIELDS_END = 0, 7

DEFAULT_START_TIME = "2016-01-01 00:00:00.000000"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class INTERVAL(Enum):
    INTERVAL_1_MINUTE = "1m"
    INTERVAL_3_MINUTE = "3m"
    INTERVAL_5_MINUTE = "5m"
    INTERVAL_15_MINUTE = "15m"
    INTERVAL_30_MINUTE = "30m"

    INTERVAL_1_HOUR = "1h"
    INTERVAL_2_HOUR = "2h"
    INTERVAL_4_HOUR = "4h"
    INTERVAL_6_HOUR = "6h"
    INTERVAL_8_HOUR = "8h"
    INTERVAL_12_HOUR = "12h"

    INTERVAL_1_DAY = "1d"
    INTERVAL_3_DAY = "3d"
    INTERVAL_1_WEEK = "1w"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return str(self)


ALL_INTERVALS = [e.value for e in INTERVAL]
