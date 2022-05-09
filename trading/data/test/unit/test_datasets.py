# stdlib
from pathlib import Path
from datetime import datetime

# 3rd party

# repo
from trading.data.Dataset import PairDataset, Pairs, BTCUSDT

def test_pairs_dataset():
    df_path = Path(__file__).parents[1] / 'resources' / 'simple_pair_df.parquet'

    Pairs.add_or_update_dataset(key=BTCUSDT, dataset=PairDataset(df_path))

    ohlc = Pairs[BTCUSDT].ohlc.from_timestamp(datetime(2020, 1, 1, 1, 48))

    assert ohlc == (48.0, 48.0, 48.0, 48.0)



