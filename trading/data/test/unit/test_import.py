# stdlib
from pathlib import Path
from datetime import datetime

# 3rd party

# repo
from trading.data import ALL_INTERVALS, INTERVAL_15_MINUTE
from trading.data import Binance


def test_import():
    # Load BTCUSDT (defaults to 1 minute candles)
    (
        Binance()
            .with_save_dir('.data/binance')
            .with_symbols(['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'ETHBTC', 'ADABTC', 'HBARUSDT', 'HBARBTC'])
            .with_intervals([INTERVAL_15_MINUTE])
            .get_or_update()
    )



