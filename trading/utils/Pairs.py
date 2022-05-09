'''Pair- related utilities
'''
# %% Libraries
# stdlib
from typing import Optional

# 3rd party
from pydantic import BaseModel, validate_arguments

# repo


class Asset(BaseModel):
    name: str

    def __hash__(self) -> int:
        return hash(self.name)


# %% Pair Class
class Pair(BaseModel):
    asset: Asset
    base: Asset

    last_open: Optional[float] = None
    last_high: Optional[float] = None
    last_low: Optional[float] = None
    last_close: Optional[float] = None

    def __hash__(self) -> int:
        return hash(self.asset) + hash(self.base)

    @validate_arguments
    def update_ohlc(
        self,
        o: float,
        h: float,
        l: float,
        c: float
    ):
        self.last_open = o
        self.last_high = h
        self.last_low = l
        self.last_close = c


# %% Pairs
BTC = Asset(name='BTC')
USDT = Asset(name='USDT')

BTCUSDT = Pair(
    asset=BTC,
    base=USDT,
)
