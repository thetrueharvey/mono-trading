"""Subcomponent that houses all data-related functionality, particularly:

- Fetching data from an exchange
"""
# %% Imports
# stdlib
from typing import Optional, List, Callable, Any
from typing_extensions import Self
import threading
import asyncio

# 3rd party

# repo
from .binance.constants import *
from .binance.functional import get_or_update as binance_get_or_update

# %% Utility
class Binance:
    def __init__(self):
        self.save_dir: Optional[str] = None
        self.symbols: Optional[List[str]] = None
        self.intervals: Optional[List[str]] = None

    def with_save_dir(self, save_dir: str) -> Self:
        self.save_dir = save_dir
        return self

    def with_symbols(self, symbols: Optional[List[str]]) -> Self:
        self.symbols = symbols
        return self

    def with_intervals(self, intervals: Optional[List[str]]) -> Self:
        self.intervals = intervals
        return self

    def get_or_update(self):
        run_async(
            binance_get_or_update,
            symbols=self.symbols,
            save_dir=self.save_dir,
            intervals=self.intervals
        )


class RunThread(threading.Thread):
    def __init__(self, func: Callable[..., Any], args: Any, kwargs: Any):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def run(self):
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))


def run_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(func(*args, **kwargs))

