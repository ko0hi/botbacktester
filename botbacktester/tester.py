from typing import Callable

import numpy as np
import pandas as pd
import tqdm
import logging

from .items import Position, Order, OpenOrder, CloseOrder, reset_id_counter
from .enums import SettleType, ExecutionType
from .evaluate import evaluation_set1
from .status import Status
from .utils import (
    DEFAULT_EXPIRE_SECONDS,
    debug_log,
    get_log_level,
    set_log_level,
)


class BackTester:
    def __init__(self, df, log_level=logging.INFO):
        assert df.index.name == "timestamp"
        assert isinstance(df.index, pd.DatetimeIndex)

        self._df = df.sort_index().reset_index()
        self._data = self._df.to_dict(orient="records")
        self._status, self._order_history, self._position_history, self._cur_i = (
            None,
            None,
            None,
            None,
        )

        set_log_level(log_level)

    def start(self):
        self.reset()

        iterator = range if get_log_level() == logging.DEBUG else tqdm.trange
        bar = iterator(0, len(self._data))

        for i in bar:
            self._cur_i = i
            self._on_step()
            yield i, self._data[self._cur_i]
        self.__clean_up()

    def reset(self):
        self._status = Status()
        self._order_history = []
        self._position_history = []
        self._cur_i = None
        reset_id_counter()

    def entry(
        self,
        side,
        exec_type,
        price: float = -1,
        expire_seconds: int = DEFAULT_EXPIRE_SECONDS,
        market_price: str = "open",
        market_slippage: int = 0,
    ):
        oo = OpenOrder(
            self.__get_entry_time(),
            side,
            exec_type,
            price,
            expire_seconds,
            market_price,
            market_slippage,
        )
        debug_log("ORDER ENTRY", oo)

        self._status.add_order(oo)
        self._order_history.append(oo)

        return oo

    def exit(
        self,
        position: Position,
        exec_type: ExecutionType,
        *,
        price: float = -1,
        losscur_price: float = -1,
        expire_seconds: int = DEFAULT_EXPIRE_SECONDS,
        market_price: str = "open",
        market_slippage: int = 0,
        update_fn_or_price_key: Callable[[dict, "CloseOrder"], None] = None,
        entry_delay_seconds: int = 0,
        market_entry_fn: Callable[[dict, "CloseOrder"], bool] = None,
        force_market_entry_seconds: int = float("inf"),
        keep_expired_orders: bool = False,
    ):
        co = CloseOrder(
            self.__get_entry_time(),
            position,
            exec_type,
            price=price,
            losscut_price=losscur_price,
            expire_seconds=expire_seconds,
            market_price_key=market_price,
            market_slippage=market_slippage,
            update_fn_or_price_key=update_fn_or_price_key,
            entry_delay_seconds=entry_delay_seconds,
            market_entry_fn=market_entry_fn,
            force_market_entry_seconds=force_market_entry_seconds,
            keep_expired_orders=keep_expired_orders,
        )
        debug_log("ORDER EXIT", co)

        self._status.add_order(co)
        self._order_history.append(co)

        return co

    def _on_step(self):
        item = self._data[self._cur_i]

        debug_log("STEP", self.__step_repr())
        debug_log("ITEM", item)

        for o in self._status.orders():
            o._on_step(item)

            if isinstance(o, OpenOrder):
                if o.is_executed:
                    p = Position(o)
                    self._status.add_position(p)
                    debug_log("POSITION", p)

            elif isinstance(o, CloseOrder):
                pass

            else:
                raise RuntimeError(f"Unsupported settle_type: {o.settle_type}")

        self.__update_status()

        debug_log(f"UPDATE STATUS", self.__step_repr())

    def orders(self, side=None, settle_type=None, exec_type=None) -> list[Order]:
        return self._status.orders(side, settle_type, exec_type)

    def positions(self, side=None, non_closing=False) -> list[Position]:
        return self._status.positions(side, non_closing)

    def get_result_df(self):
        assert len(self.position_history) > 0, "Results not found"
        df = pd.DataFrame([p.as_dict() for p in self.position_history])

        df["timestamp"] = df.oo_entried_at
        df["side"] = df.oo_side
        df.set_index("timestamp", inplace=True)
        df.drop(columns=["oo_settle_type", "co_settle_type"], inplace=True)

        df["gain_buy"] = np.where(df.side == "BUY", df.gain, 0)
        df["gain_sell"] = np.where(df.side == "SELL", df.gain, 0)

        return df

    def report(self, **kwargs):
        df_result = self.get_result_df()
        evaluation_set1(df_result, **kwargs)
        return df_result

    @classmethod
    def enable_debug_log(cls):
        set_log_level("DEBUG")

    @classmethod
    def disable_debug_log(cls):
        set_log_level("INFO")

    @property
    def status(self) -> Status:
        return self._status

    @property
    def order_history(self) -> list[Order]:
        return self._order_history

    @property
    def position_history(self) -> list[Position]:
        return self._position_history

    def __get_entry_time(self):
        return self._data[self._cur_i]["timestamp"]

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
                co = CloseOrder(
                    last["timestamp"], p, ExecutionType.MARKET, market_price_key="close"
                )
                co._executed(last)
                p.close(self._data[-1], co)

        self.__update_status()

        assert self._status.order_num == 0
        assert self._status.position_num == 0

    def __update_status(self):
        self._status.clear_done_orders()
        closed_positions = self._status.clear_closed_positions()

        if len(closed_positions):
            self._position_history += closed_positions

    def __step_repr(self):
        return f"{self._cur_i}/{self._status.order_num}/{self._status.position_num}"
