'''Dataset API

Provides functionality for:

Timestamps

Pair data
'''
# %% Libraries
# stdlib
from typing import Any, Protocol, Dict, Tuple, List, ClassVar, Type
from pathlib import Path
from datetime import datetime

# 3rd party
from pydantic import validate_arguments
from polars import LazyFrame, DataFrame, scan_parquet, DataType, datatypes, col

# repo
from trading.utils.Pairs import BTCUSDT, Pair, Asset, BTC, USDT
from trading.data.binance.constants import INTERVAL


# %% Schemas
class Schema(Protocol):
    column_types: ClassVar[Dict[str, Type[DataType]]]

    @classmethod
    def validate(cls, df: LazyFrame[Any]):
        _df: DataFrame = df.fetch(2)

        validation_set = set(cls.column_types.keys())
        actual_set = set(df.columns)

        extra_columns = validation_set.difference(actual_set)
        missing_columns = actual_set.difference(validation_set)

        bad_dtypes: Dict[str, Tuple[Type[DataType], Type[DataType]]] = {}
        dtype_iter = zip(cls.column_types.items(), _df.select([*cls.column_types.keys()]).dtypes)
        for (column, expected_dtype), found_dtype in dtype_iter:
            if expected_dtype != found_dtype:
                bad_dtypes[column] = (expected_dtype, found_dtype)

        errors: List[str] = []
        if bad_dtypes:
            msg = [f'{col}: {expected} != {found}' for col, (expected, found) in bad_dtypes.items()]
            msg = "The following columns have the incorrect datatype\n\t{}".format('\n\t'.join(msg))
            errors.append(msg)

        if extra_columns:
            msg = "The following columns are not being validated\n\t{}".format('\n\t'.join(extra_columns))
            errors.append(msg)

        if missing_columns:
            msg = "The following columns are not in the DataFrame\n\t{}".format('\n\t'.join(missing_columns))
            errors.append(msg)

        if errors:
            raise RuntimeError("Validation failed\n{}".format('\n'.join(errors)))


# %% Protocol
class Dataset(Protocol):
    '''Dataset API Protocol
    '''
    pass


class ParquetDataset(Dataset):
    '''Defines a dataset that reads from a parquet file
    '''
    schema: Schema

    @validate_arguments
    def __init__(
        self,
        parquet: Path,
    ):
        assert parquet.exists()
        self.parquet = parquet

        self.df: LazyFrame[Any] = scan_parquet(self.parquet)
        self.schema.validate(self.df)

        self.__post_init__()

    def __post_init__(self):
        ...


class PairSchema(Schema):
    column_types = {
        'timestamp': datatypes.Datetime,
        **{col: datatypes.Float32 for col in ('open', 'high', 'low', 'close')}
    }


class _OHLC:
    _timestamp = col('timestamp')
    _o = col('open')
    _h = col('high')
    _l = col('low')
    _c = col('close')

    def __init__(self, df: LazyFrame[Any]):
        self.df = df

    @validate_arguments
    def from_timestamp(self, timestamp: datetime) -> Tuple[float]:
        return (
            *self.df
                .filter(self._timestamp == timestamp)
                .select([self._o, self._h, self._l, self._c])
                .collect()
                .to_dicts()
                [0]
                .values(),
        )


class PairDataset(ParquetDataset):
    schema = PairSchema

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._OHLC = _OHLC(df=self.df)

    @property
    def ohlc(self) -> _OHLC:
        return self._OHLC


# %% Pair datasets
class DatasetManager(Dataset):
    datasets: Dict[Any, Any]

    def __init__(self, datasets: Dict[Any, Any]):
        self.datasets = datasets

    def __getitem__(self, key: ...) -> Dataset:
        return self.datasets[key]

    def add_or_update_dataset(self, key: ..., dataset: ...):
        self.datasets[key] = dataset


class Pairs(DatasetManager):
    def __init__(self, datasets: Dict[Pair, PairDataset]):
        self.datasets = datasets

    def __getitem__(self, key: Pair) -> PairDataset:
        return self.datasets[key]

    def add_or_update_dataset(self, key: Pair, dataset: PairDataset):
        self.datasets[key] = dataset


class IntervalPairs(DatasetManager):
    def __init__(self, datasets: Dict[INTERVAL, Pairs]):
        self.datasets = datasets

    def __getitem__(self, key: INTERVAL) -> Pairs:
        return self.datasets[key]

    def add_or_update_dataset(self, key: INTERVAL, dataset: Pairs):
        self.datasets[key] = dataset
