"""Functional interface for fetching and managing data from Binance
"""
# %% Setup
# stdlib
from typing import List, Optional
from pathlib import Path
from datetime import datetime, timedelta

# 3rd party
import polars as pl
import orjson

# custom
from .client import BinanceClient
from ...utils.loading import Loader
from .constants import *


# %% General utilities
def create_empty_schema():
    data = [
        pl.Series("Open time", [], dtype=pl.Datetime),
        pl.Series("Open", [], dtype=pl.Float64),
        pl.Series("High", [], dtype=pl.Float64),
        pl.Series("Low", [], dtype=pl.Float64),
        pl.Series("Close", [], dtype=pl.Float64),
        pl.Series("Volume", [], dtype=pl.Float64),
        pl.Series("Close time", [], dtype=pl.Datetime)
    ]

    return pl.DataFrame(data)


def print_logger(timestamp: datetime, message: str, rows: Optional[int] = None):
    print(f"{timestamp}: {message} | {rows} rows")


# %% BinanceData functions
# Load klines
async def update_kline_data(
    symbol: str, 
    save_dir: Path, 
    default_start: str = DEFAULT_START_TIME,
    interval: str = INTERVAL_1_MINUTE
):
    with Loader(f"Fetching all data for {symbol} | {interval}..."):
        save_path = save_dir / f"datasets"
        if not save_path.exists():
            save_path.mkdir(parents=True)

        try:
            kline_df = pl.read_parquet(source=save_path / f"{symbol}_{interval}.parquet")
            start_time = str(datetime.fromtimestamp(kline_df["Close time"].max() / 1e3) + timedelta(milliseconds=1)) + ".000000"
        except FileNotFoundError:
            #logger(timestamp=datetime.now(), message=f"Fetching all data for {symbol}")
            kline_df = create_empty_schema()
            start_time = default_start

        end_time = str(datetime.now())

        client = BinanceClient()

        kline_df_ = await client.get_kline_data(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time
        )

        print_logger(
            timestamp=datetime.now(),
            message=f"Fetched data for {symbol} | {interval} | {start_time} to {end_time}",
            rows=len(kline_df_) if kline_df_ is not None else None
        )

        if kline_df_ is not None:
            kline_df.vstack(kline_df_, in_place=True)
            kline_df.sort(by="Open time").distinct().to_parquet(file=save_path / f"{symbol}_{interval}.parquet")


# %% Update Pipeline
async def kline_pipeline(
    symbols: Optional[List[str]] = None, 
    save_dir: str = ".data/binance",
    intervals: List[str] = [INTERVAL_1_MINUTE],
):
    if isinstance(intervals, str):
        intervals = [intervals]

    save_path = Path(save_dir)
    exchange_save_path = save_path / "exchange.parquet"

    if symbols is None:
        if not save_path.exists():
            print_logger(
                timestamp=datetime.now(),
                message="Creating a directory for Binance metadata"
            )
            save_path.mkdir(parents=True)

        exchange_df = await BinanceClient().get_exchange_data()
        exchange_df.to_parquet(file=exchange_save_path)

        symbols = exchange_df.sort("liquidity", reverse=True)["symbol"].to_list()

    for symbol in symbols:
        for interval in intervals:
            await update_kline_data(
                symbol=symbol, 
                save_dir=save_path, 
                interval=interval
            )


# %% Generic JSON request
async def _generic_json_request(session, request):
    async with await session.get(request) as resp:
        result = await resp.text()
    return orjson.loads(result)


# %% Symbol information
async def get_info(session, symbol: str = None):
    base_request = f"""{BASE_URL}/exchangeInfo"""
    if symbol is None:
        return await _generic_json_request(session=session, request=base_request)

    symbol_request = f"""{base_request}?symbol={symbol}"""
    return await _generic_json_request(session=session, request=symbol_request)


# %% Symbol ticker
async def get_ticker(session, symbol: str = None):
    base_request = f"""{BASE_URL}/ticker/24hr"""

    if symbol is None:
        return await _generic_json_request(session=session, request=base_request)

    symbol_request = f"""{base_request}?symbol={symbol}"""
    return await _generic_json_request(session=session, request=symbol_request)


# %% Symbol klines
async def get_klines(session, symbol: str, interval: str, start_time: int, end_time: int):
    base_request = f"""{BASE_URL}/klines?symbol={symbol}&interval={interval}&limit=1000"""

    if start_time is None and end_time is None:
        return await _generic_json_request(session=session, request=base_request)

    full_request = f"""{base_request}&startTime={start_time}&endTime={end_time}"""
    return await _generic_json_request(session=session, request=full_request)