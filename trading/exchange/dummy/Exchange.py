'''API for a dummy exchange
'''
# %% Libraries
# stdlib
from typing import Dict, List, Set, Tuple, Callable, Any, Optional
from enum import Enum, auto
from datetime import datetime

# 3rd party
from pydantic import BaseModel, validate_arguments

# repo
# TODO: Get config
from data import Dataset
from data.Dataset import Pair
from trading.utils.Pairs import Pair, Asset, BTC, USDT


# %% Enums
class Side(Enum):
    Buy = auto()
    Sell = auto()


class OrderStatus(Enum):
    Open = auto()
    Cancelled = auto()
    Completed = auto()


class Balance(BaseModel):
    available: float
    reserved: float

    def reserve(self, amount: float) -> None:
        assert amount <= self.available
        self.available -= amount
        self.reserved += amount


# %% Order Class
class Order(BaseModel):
    pass


class SimpleOrder(Order):
    placed: datetime
    side: Side
    pair: Pair
    status: OrderStatus
    amount: float
    price: float

    status: OrderStatus = OrderStatus.Open


class LogicOrder(Order):
    timestamp: datetime
    side: Side
    pair: Pair
    amount: float

    def step(self) -> None:
        '''Executes the order logic at each step
        '''
        raise NotImplementedError()


class StepOp(BaseModel):
    op: Callable[..., None]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def execute(self):
        self.op(*self.args, **self.kwargs)


# %% Exchange Class
class Exchange:
    timestamp: datetime
    pairs: List[Pair]

    balances: Dict[Asset, Balance]
    closed_orders: Set[Order]
    active_orders: Set[Order]
    cancelled_orders: Set[Order]

    commission: float
    _ops: List[StepOp]


    def __init__(
        self,
        commission: float
    ):  
        self.commission = commission
        self.__post_init__()

    def __post_init__(self):
        self.balances = {
            USDT: Balance(
                available=10_000.0,
                reserved=0.0,
            ),
            BTC: Balance(
                available=0.0,
                reserved=0.0
            )
        }
        self.closed_orders = set()
        self.active_orders = set()
        self.cancelled_orders = set()

        self._ops = []

        self._timestamp_data: Dataset.Dataset
        self._pair_data: Dataset.Dataset

    def step(
        self
    ) -> None:
        '''Moves the exchange 1 step forward in time, updating all pairs last prices, then executes (in order) all operations
        '''
        raise NotImplementedError()

        while len(self._internal_ops) > 0:
            op = self._internal_ops.pop(0)
            op.execute()

        while len(self._ops) > 0:
            op = self._ops.pop(0)
            op.execute()

    def _check_order(self, order: SimpleOrder) -> None:
        low = order.pair.last_low
        high = order.pair.last_high

        assert low is not None and high is not None

        if order.side == Side.Buy:
            if low < order.price:
                self.balances[order.pair.base].reserved -= order.amount * order.price
                self.balances[order.pair.asset].available += order.amount

                # TODO: Update the order information

        if order.side == Side.Sell:
            if high > order.price:
                self.balances[order.pair.asset].reserved -= order.amount
                self.balances[order.pair.base].available += order.amount * order.price

                # TODO: Update the order information

        '''
        When an order is placed, it is saying 'I want to buy x of an asset at y price'
        Hence, the converted amount of the base must be reserved
        '''

    def _update_timestamp(self):
        self.timestamp = self._timestamp_data.get_next()
    
    def _update_pair(self, pair: Pair) -> None:
        '''Updates the pair information with the data of the latest step
        '''
        ohlc = self._pair_data[pair].ohlc.for_timestamp()
        pair.update_ohlc(*ohlc)

    @property
    def _internal_ops(self) -> List[StepOp]:
        return [
            StepOp(self._update_timestamp),
            *map(lambda pair: StepOp(self._update_pair, pair=pair), self.pairs)
        ]

    def cancel(
        self,
        order: Order
    ):
        assert order not in self.cancelled_orders

        self.active_orders.remove(order)
        self.cancelled_orders.add(order)

    def _buy(self, order: SimpleOrder):
        assert order not in self.active_orders
        self.balances[order.pair.base].reserve(order.amount * order.price)

        self.active_orders.add(order)

    def _sell(self, order: SimpleOrder):
        assert order not in self.active_orders
        self.balances[order.pair.asset].reserve(order.amount)

        self.active_orders.add(order)

    def add_op(
        self,
        op: Callable[..., None],
        *args: Any,
        **kwargs: Any,
    ):
        '''Adds an operation to the exchange stack
        
        These operations will be executed after the next step
        '''
        self._ops.append(
            StepOp(
                op=op,
                args=args,
                kwargs=kwargs
            )
        )


    