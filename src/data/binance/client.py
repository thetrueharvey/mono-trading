"""Class for querying the Binance API
"""
# %% Setup
# stdlib
from typing import Callable
from time import monotonic
from datetime import datetime
from itertools import chain

import asyncio

# 3rd party
import aiohttp
import polars as pl

# custom
from .constants import *


# configuration
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class RateLimiter:
    RATE = 8
    MAX_TOKENS = 8

    def __init__(self, client):
        self.client = client
        self.tokens = self.MAX_TOKENS
        self.updated_at = monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.get(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens <= 1:
            self.add_new_tokens()
            await asyncio.sleep(1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.RATE
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.MAX_TOKENS)
            self.updated_at = now


def get_total_minutes(interval):
    if interval[-1] == 'm':
        time = int(interval[:-1])

    if interval[-1] == 'h':
        time = 60 * int(interval[:-1])

    if interval[-1] == 'd':
        time = 24 * 60 * int(interval[:-1])

    if interval[-1] == 'w':
        time = 7 * 24 * 60 * int(interval[:-1])

    if interval[-1] == 'M':
        time = 30 * 24 * 60 * int(interval[:-1])

    return time

class BinanceClient:
    def __init__(self, logger: Callable = None):
        self.logger = logger

    @staticmethod
    def _parse_symbol_data(info_df: pl.DataFrame, data_df: pl.DataFrame):
        info_df = info_df.filter(pl.col('quoteAsset').str.contains('BTC|USDT'))

        df = (info_df
              .join(data_df, on='symbol')
              .select(['symbol', 'baseAsset', 'quoteAsset', 'weightedAvgPrice', 'volume'])
              .with_column((pl.col('weightedAvgPrice').cast(pl.datatypes.Float64) *
                            pl.col('volume').cast(pl.datatypes.Float64)).alias('liquidity'))
              .filter(pl.col('liquidity') > 0)
              )

        return df

    async def get_all_symbol_data(self, session):
        from .functional import (get_info, get_ticker)

        futures = (
            get_info(session=session), 
            get_ticker(session=session)
        )

        ticker_info, ticker_data = await asyncio.gather(*futures)

        for record in ticker_info['symbols']:
            for field in SYMBOL_INFO_JSON_FIELDS:
                record[field] = str(record[field])

        info_df = pl.from_dicts(dicts=ticker_info['symbols']).lazy()
        data_df = pl.from_dicts(dicts=ticker_data).lazy()

        return self._parse_symbol_data(info_df, data_df).collect()

    async def get_all_klines(self, session, symbol: str, interval: str, start_time: int = None, end_time: int = None):
        # Determine the number of 1000 `interval` timestamps required
        if start_time and end_time is not None:
            start_time = int(datetime.strptime(start_time, DATETIME_FORMAT).timestamp()) * 1000
            end_time = int(datetime.strptime(end_time, DATETIME_FORMAT).timestamp()) * 1000

        times = [(start_time, start_time + 1000 * 60 * 1000)]
        while True:
            times.append((times[-1][-1], times[-1][-1] + 1000 * 60 * 1000))
            if times[-1][-1] > end_time:
                break

        async def wrap_kline_request(symbol, interval, times):
            from .functional import get_klines

            tasks = []
            for start_time, end_time in times:
                future = asyncio.ensure_future(
                    get_klines(
                        session=session,
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time,
                        end_time=end_time
                    )
                )
                tasks.append(future)

            return await asyncio.gather(*tasks)

        klines = await wrap_kline_request(symbol=symbol, interval=interval, times=times)
        klines = list(chain.from_iterable(klines))
        klines = [{field: value for field, value in zip(KLINE_FIELDS, record[KLINE_FIELDS_START:KLINE_FIELDS_END])}
                  for record in klines]

        if len(klines) == 0:
            return

        kline_df = self.process_klines(kline_dict=klines)

        return kline_df

    @staticmethod
    def process_klines(kline_dict):
        kline_df = pl.from_dicts(kline_dict).lazy()
        kline_df = (kline_df
                    .with_column(pl.col('Open time').cast(pl.datatypes.Datetime))
                    .with_column(pl.col('Close time').cast(pl.datatypes.Datetime))
                    .with_column(pl.col('Open').cast(pl.datatypes.Float64))
                    .with_column(pl.col('High').cast(pl.datatypes.Float64))
                    .with_column(pl.col('Low').cast(pl.datatypes.Float64))
                    .with_column(pl.col('Close').cast(pl.datatypes.Float64))
                    .with_column(pl.col('Volume').cast(pl.datatypes.Float64))
                    .collect()
                    )

        return kline_df

    async def async_get_kline_data(self, symbol, interval, start_time, end_time):
        async with aiohttp.ClientSession() as session:
            session = RateLimiter(session)
            kline_df = await self.get_all_klines(
                session=session,
                symbol=symbol,
                interval=interval,
                start_time=start_time,
                end_time=end_time
            )
        return kline_df

    async def async_get_all_symbol_data(self):
        async with aiohttp.ClientSession() as session:
            session = RateLimiter(session)
            exchange_df = await self.get_all_symbol_data(session=session)

        return exchange_df

    async def get_kline_data(self, symbol, interval, start_time, end_time) -> pl.DataFrame:
        return await self.async_get_kline_data(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time
        )

    async def get_exchange_data(self) -> pl.DataFrame:
        return await self.async_get_all_symbol_data()