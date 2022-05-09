# stdlib
from pathlib import Path
from datetime import datetime

# 3rd party

# repo
from trading.data.Dataset import PairDataset

def test_pair_validation():
    df_path = Path(__file__).parents[1] / 'resources' / 'simple_pair_df.parquet'

    PairDataset(df_path)


def test_ohlc():
    df_path = Path(__file__).parents[1] / 'resources' / 'simple_pair_df.parquet'

    ds = PairDataset(df_path)

    ohlc = ds.ohlc.from_timestamp(datetime(2020, 1, 1, 1, 48))

    assert ohlc == (48.0, 48.0, 48.0, 48.0)
