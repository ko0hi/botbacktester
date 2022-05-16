from typing import Callable

import pandas as pd
import logging

from .items import Position, Order, OpenOrder, CloseOrder
from .enums import SettleType, OrderStatus, ExecutionType
from .status import Status
from .utils import DEFAULT_EXPIRE_SECONDS, set_log_level


class BackTester:
    def __init__(self, df, log_level=logging.DEBUG):
        assert df.index.name == "timestamp"
        assert isinstance(df.index, pd.DatetimeIndex)

        self._df = df.sort_index().reset_index()
        self._data = self._df.to_dict(orient='records')
        self._status = Status()
        self._order_history = []
        self._closed_positions = []

        self._cur_i = None

        set_log_level(log_level)

    def start(self):
        for i in range(0, len(self._data)):
            self._cur_i = i
            self._on_step()
            yield i, self._data[self._cur_i]
        self.__clean_up()

    def entry(
            self,
            side,
            exec_type,
            price: float = None,
            expire_seconds: int = DEFAULT_EXPIRE_SECONDS,
            market_price: str = 'open',
            market_slippage: int = 0
    ):
        oo = OpenOrder(self.__get_entry_time(), side, exec_type, price, expire_seconds, market_price, market_slippage)
        self._status.add_order(oo)
        self._order_history.append(oo)
        return oo

    def exit(
            self,
            position: Position,
            exec_type: ExecutionType,
            *,
            price: float = None,
            losscur_price: float = None,
            expire_seconds: int = DEFAULT_EXPIRE_SECONDS,
            market_price: str = 'open',
            market_slippage: int = 0,
            update_fn_or_price_key: Callable[[dict, 'CloseOrder'], None] = None,
            entry_delay_seconds: int = 0,
            market_entry_fn: Callable[[dict, 'CloseOrder'], bool] = None,
            force_market_entry_seconds: int = float('inf'),
            keep_expired_orders: bool = False
    ):
        co = CloseOrder(
            self.__get_entry_time(), position, exec_type, price=price,
            losscut_price=losscur_price, expire_seconds=expire_seconds,
            market_price_key=market_price, market_slippage=market_slippage,
            update_fn_or_price_key=update_fn_or_price_key, entry_delay_seconds=entry_delay_seconds,
            market_entry_fn=market_entry_fn, force_market_entry_seconds=force_market_entry_seconds,
            keep_expired_orders=keep_expired_orders
        )
        self._status.add_order(co)
        self._order_history.append(co)
        return co

    def _on_step(self):
        item = self._data[self._cur_i]

        for o in self._status.orders():
            o._on_step(item)

            if isinstance(o, OpenOrder):
                if o.is_executed:
                    self._status.add_position(Position(o))

            elif isinstance(o, CloseOrder):
                pass

            else:
                raise RuntimeError(f"Unsupported settle_type: {o.settle_type}")

        self.__update_status()

    def orders(self, side=None, settle_type=None, exec_type=None) -> list[Order]:
        return self._status.orders(side, settle_type, exec_type)

    def positions(self, side=None, non_closing=False) -> list[Position]:
        return self._status.positions(side, non_closing)

    @property
    def status(self) -> Status:
        return self._status

    @property
    def order_history(self) -> list[Order]:
        return self._order_history

    @property
    def closed_positions(self) -> list[Position]:
        return self._closed_positions

    def __get_entry_time(self):
        return self._data[self._cur_i]['timestamp']

    def __clean_up(self):
        last = self._data[-1]

        for o in self.orders():
            if o.settle_type == SettleType.OPEN:
                o._expired(last)
            else:
                o._expired_and_executed(last, market_price_key="close")

        for p in self.positions():
            # is_closingの場合、上でcloseされているはず
            assert not p.is_closing

            # CloseOrderが未注文のポジション
            if not p.is_closed:
                co = CloseOrder(last['timestamp'], p, ExecutionType.MARKET, market_price_key='close')
                co._executed(last)
                p.close(self._data[-1], co)

        self.__update_status()

        assert self._status.order_num == 0
        assert self._status.position_num == 0

    def __update_status(self):
        done_orders = self._status.clear_done_orders()
        closed_positions = self._status.clear_closed_positions()

        if len(closed_positions):
            self._closed_positions += closed_positions
