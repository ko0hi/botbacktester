from __future__ import annotations
from typing import Callable, Union, NamedTuple

import logging
import itertools
import pandas as pd

from datetime import timedelta

from .enums import Side, ExecutionType, SettleType, OrderStatus
from .utils import get_logger


# 求まるexpire_timeがテストデータに収まらないくらい先になるように十分大きい値、かつ、
# ``pd.to_datetime``が受け付ける値である必要がある
DEFAULT_EXPIRE_SECONDS = 1e+8

ID_COUNTER = itertools.count()


DEBUG = False
def enable_debug_print():
    global DEBUG
    DEBUG = True


def disable_debug_print():
    global DEBUG
    DEBUG = False


logger = get_logger()


def elapsed_time(begin, end, metric='seconds'):
    delta = (end - begin).seconds
    if metric == 'seconds':
        return delta
    elif metric == 'minutes':
        return delta / 60
    elif metric == 'hours':
        return delta / 3600
    else:
        raise RuntimeError(f"Unsupported: {metric}")


def check_limit(item: dict, side: Side, price: float):
    if side == Side.BUY:
        return item['low'] <= price
    else:
        return item['high'] >= price

def check_stop(item: dict, side: Side, price: float):
    if side == Side.BUY:
        return item['high'] >= price
    else:
        return item['low'] <= price


class Position:
    def __init__(self, open_order: OpenOrder):
        self.open_order: OpenOrder = open_order
        self.closing_order: CloseOrder = None
        self.close_order: CloseOrder = None
        self.close_item: dict = None
        self._id = next(ID_COUNTER)

    def __repr__(self):
        if self.close_order is None:
            return f"{self.__class__.__name__}(side={self.open_order.side}, open_price={self.open_price})"
        else:
            return f"{self.__class__.__name__}(id={self._id}/" \
                   f"side={self.open_order.side}/" \
                   f"is_closed={self.is_closed}/" \
                   f"gain={self.gain:.3f}/" \
                   f"total_time={self.total_execution_time()}(" \
                   f"{self.open_execution_time()}/{self.close_execution_time()}))"


    def set_closing_order(self, closing_order: CloseOrder):
        self.closing_order = closing_order

    def clear_closing_order(self):
        self.closing_order = None

    def close(self, item: dict, close_order: 'CloseOrder'):
        # 約定した場合 or シミュレーションが終了した場合に呼ばれる。
        # 後者の場合、cur・close_orderともに最後のものが入れられる。
        self.close_item = item
        self.close_order = close_order

    def as_dict(self):
        d = {}
        d.update({f'oo_{k}': v for (k, v) in self.open_order.as_dict().items()})
        if self.close_order is not None:
            d.update({f'co_{k}': v for (k, v) in self.close_order.as_dict().items()})

        d['gain'] = self.gain

        return d

    @property
    def id(self):
        return self._id

    @property
    def gain(self):
        gain = self.close_price / self.open_price - 1

        if self.open_order.side == Side.SELL:
            gain = gain * -1

        gain -= self.open_order.fee - self.close_order.fee

        return gain

    @property
    def side(self):
        return self.open_order.side

    @property
    def size(self):
        return self.open_order.size

    @property
    def open_price(self):
        return self.open_order.price

    @property
    def close_price(self):
        if self.is_closed:
            return self.close_order.price
        else:
            # closeされなかった場合は最終アイテムで計算される
            return self.close_item['close']

    @property
    def is_closing(self):
        return self.closing_order is not None

    @property
    def is_closed(self):
        return self.close_order is not None

    @property
    def open_item(self):
        return self.open_order.executed_item

    def open_execution_time(self, unit='seconds'):
        return self.open_order.execution_time(unit)

    def close_execution_time(self, unit='seconds'):
        if self.is_closed:
            # 決済された場合は決済注文の約定時間
            return self.close_order.execution_time(unit)
        else:
            # 決済されなかった場合は決済注文の発注時間から最後のseriesまで。
            return elapsed_time(
                self.close_order.entry_time, self.close_item.name
            )

    def total_execution_time(self, unit='seconds'):
        return elapsed_time(
            self.open_order.entry_time, self.close_item['timestamp']
        )



class Order:
    def __init__(
            self,
            side: Side,
            exec_type: ExecutionType,
            settle_type: SettleType,
            *,
            price: float = None,
            size: int = 1,
            entry_time: 'datetime.datetime' = None,
            expire_seconds: int = float('inf'),
            market_price_key: str = 'open',
            market_slippage: int = 0
    ):
        # イベント管理用の変数なのでprotectedにしておく
        self._executed_item = None
        self._expired_item = None
        self._expire_time = None
        self._expire_seconds = expire_seconds
        self._entry_time = None
        self._exec_type_original = exec_type
        self._status = OrderStatus.ORDERING
        self._id = next(ID_COUNTER)

        if entry_time is not None:
            self._set_entry_and_expire_time(entry_time)
        else:
            # Open時は``entry_time``必須
            assert settle_type != SettleType.OPEN

        assert side in Side, f"Unsupported side: {side}"
        self.side = side

        assert exec_type in ExecutionType, f"Unsupported exec_type: {exec_type}"
        self.exec_type = exec_type

        assert settle_type in SettleType, f"Unsupported settle_type: {settle_type}"
        self.settle_type = settle_type

        if self.exec_type == ExecutionType.LIMIT:
            assert price is not None, f"``price`` is required when exec_type={ExecutionType.LIMIT}"
        self.price = price

        self.size = size

        self._market_price_key = market_price_key
        self.market_slippage = market_slippage

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self._id}/" \
               f"entry_at={self.entried_at}/" \
               f"expired_at={self.expired_at}/" \
               f"price={self.price}/" \
               f"side={self.side}/" \
               f"exec_type={self.exec_type}/" \
               f"executed={self.is_executed}/" \
               f"expired={self.is_expired})"

    def execution_time(self, metric='seconds'):
        if self.is_executed:
            return elapsed_time(self.entry_time, self.executed_at, metric)
        else:
            return None

    def as_dict(self):
        return {
            "side": self.side.name,
            "exec_type": self.exec_type.name,
            "settle_type": self.settle_type.name,
            "price": self.price,
            "size": self.size,
            "is_executed": self.is_executed,
            "status": self.status.name,
            "entried_at": self.entried_at,
            "executed_at": self.executed_at,
            "expired_at": self.expired_at,
            "fee": self.fee
        }

    @property
    def id(self):
        return self._id

    @property
    def status(self) -> OrderStatus:
        return self._status

    @property
    def is_done(self):
        return self._status != OrderStatus.ORDERING

    @property
    def is_executed(self):
        return self._status in [OrderStatus.EXECUTED, OrderStatus.EXPIRED_EXECUTED, OrderStatus.LOSSCUT]

    @property
    def executed_item(self):
        return self._executed_item

    @property
    def is_expired(self):
        return self._status in [OrderStatus.EXPIRED, OrderStatus.EXPIRED_EXECUTED]

    @property
    def expired_item(self):
        return self._expired_item

    @property
    def expire_time(self):
        return self._expire_time

    @property
    def expire_seconds(self):
        return self._expire_seconds

    @property
    def entry_time(self):
        return self._entry_time

    @property
    def fee(self):
        if self.executed_item is None:
            return 0
        else:
            k = "taker_fee" if self.exec_type == ExecutionType.MARKET else "maker_fee"
            return self.executed_item.get(k, 0)

    @property
    def entried_at(self) -> pd.Timestamp:
        return self._entry_time

    @property
    def executed_at(self) -> pd.Timestamp:
        if self._executed_item is not None:
            return self._executed_item['timestamp']

    @property
    def expired_at(self) -> pd.Timestamp:
        if self._expired_item is not None:
            return self._expired_item['timestamp']

    @classmethod
    def _validate_item(cls, item):
        assert isinstance(item['timestamp'], pd.Timestamp)

    def _on_step(self, item: dict):
        raise NotImplementedError

    def cancel(self):
        self._status = OrderStatus.CANCELED

    def _executed(self, item, *, force_market=False, market_price_key=None):
        self._executed_item = item

        if self.exec_type == ExecutionType.MARKET or force_market:
            self.price = self._get_market_price(item, market_price_key)
            self.exec_type = ExecutionType.MARKET

        self._status = OrderStatus.EXECUTED

        logger.debug(f"[EXECUTED ORDER] {item['timestamp']} {self}")

    def _expired(self, item: dict):
        self._expired_item = item
        self._status = OrderStatus.EXPIRED
        logger.debug(f"[EXPIRED ORDER] {item['timestamp']} {self}")

    def _check_execution(self, item: dict):
        self._validate_item(item)

        if self.exec_type == ExecutionType.MARKET:
            assert item['timestamp'] >= self._entry_time
            return True

        elif self.exec_type == ExecutionType.LIMIT:
            return check_limit(item, self.side, self.price)

        elif self.exec_type == ExecutionType.STOP:
            return check_stop(item, self.side, self.price)

        else:
            raise RuntimeError

    def _check_expiration(self, item: dict):
        self._validate_item(item)
        logger.debug(f"[CHECK EXPIRATION] cur={item['timestamp']} vs. expire_time={self.expire_time}")
        return item['timestamp'] >= self.expire_time

    def _set_entry_and_expire_time(self, entry_time: Union[str, pd.Timestamp]):
        """ エントリー時間と失効時間をセットする。

        :param entry_time: str or datetime
        :return:
        """
        if isinstance(entry_time, str):
            entry_time = pd.to_datetime(entry_time)

        self._entry_time = entry_time
        self._expire_time = self._entry_time + timedelta(seconds=self.expire_seconds)

    def _get_market_price(self, item, market_price_key=None):
        market_price_key = market_price_key or self._market_price_key
        if market_price_key == "best":
            assert "bid" in item and "ask" in item
            if self.side == Side.BUY:
                return item['ask'] + self.market_slippage
            else:
                return item['bid'] - self.market_slippage
        else:
            assert market_price_key in item
            if self.side == Side.BUY:
                return item[market_price_key] + self.market_slippage
            else:
                return item[market_price_key] - self.market_slippage


class OpenOrder(Order):
    def __init__(
            self,
            entry_time,
            side: Side,
            exec_type: ExecutionType,
            price: float = None,
            expire_seconds: int = DEFAULT_EXPIRE_SECONDS,
            market_price_key: str = 'open',
            market_slippage: int = 0
    ):
        super().__init__(
            side, exec_type, SettleType.OPEN,
            price=price,
            entry_time=entry_time,
            expire_seconds=expire_seconds,
            market_price_key=market_price_key,
            market_slippage=market_slippage
        )

    def _on_step(self, cur: dict):
        assert not self.is_done, f"Invalid order status: {self.status}"

        if self._check_execution(cur):
            self._executed(cur)
        else:
            if self._check_expiration(cur):
                self._expired(cur)


class CloseOrder(Order):
    def __init__(
            self,
            entry_time: Union[str, pd.Timestamp],
            position: Position,
            exec_type: ExecutionType,

            *,

            # 指値。
            # ``expire_seconds``の指定がない場合、シミュレーション終了までこの与えられた
            # 値で待ち続ける。
            # ``expire_seconds``の指定がある場合、``update_fn_or_price_key``に従っ
            # て失効するたびに更新されていく。
            price: float = None,

            losscut_price: float = None,

            # entry後から何秒後に失効するか
            expire_seconds: int = DEFAULT_EXPIRE_SECONDS,

            market_price_key: str = 'open',
            market_slippage: int = 0,

            # priceを動的に定めるためのもの。以下のいずれかである必要がある。
            # functionの場合: Callable[dict, CloseOrder]
            # keyの場合: dict[key]
            # （``expire_seconds``が指定、かつ、``exec_type``がMARKETでない場合必須）
            update_fn_or_price_key: Callable[[dict, 'CloseOrder'], None] = None,

            # OpenOrderが約定したタイミングからCloseOrderを出すまでの待ち時間。
            # default(0)ではOpenOrderが約定したタイミングでCloseOrderをエントリーする。
            # ただしCloseOrderが評価されるのは次の時刻からである。
            entry_delay_seconds: int = 0,

            # 成行決済の判定器。``exec_type``がMARKETの場合は以下のロジックで執行される。
            # 1. ``makert_entry_fn``がNoneの場合：``entry_delay_seconds``経過後、
            # その次のタイミングで執行。ただし、``entry_delay_seconds > expire_seconds``
            # の場合、``expire_seconds``経過時に執行。
            # 2. ``market_entry_fn``が与えられた場合：``entry_delay_seconds``待った
            # 後、``expire_seconds``が経過するまで、``market_entry_fn``で成行注文の
            # 可否を判定する。Trueが返ってきたタイミング or ``expire_seconds``経過時に
            # 執行。
            market_entry_fn: Callable[[dict, 'CloseOrder'], bool] = None,

            force_market_entry_seconds: int = float('inf'),

            keep_expired_orders: bool = False
    ):
        # if (
        #         expire_seconds < DEFAULT_EXPIRE_SECONDS and
        #         exec_type != ExecutionType.MARKET and
        #         update_fn_or_price_key is None
        # ):
        #     raise RuntimeError(
        #         "``update_fn_or_price_key`` is required when ``expire_seconds`` "
        #         "is specified and ``exec_type`` is not MARKET."
        #     )

        if (
                price is None and
                exec_type != ExecutionType.MARKET and
                update_fn_or_price_key is None
        ):
            raise RuntimeError(
                f"``update_fn_or_price_key`` is requrired when ``price`` is "
                f"not specified and ``exec_type`` is not MARKET."
            )

        super().__init__(
            position.side.reverse(),
            exec_type,
            SettleType.CLOSE,
            entry_time=entry_time,
            price=price,
            expire_seconds=expire_seconds,
            market_price_key=market_price_key,
            market_slippage=market_slippage
        )
        self._position: Position = position
        # このCloseOrderへのポインターをセット
        self._position.set_closing_order(self)

        self._losscut_price = losscut_price
        self._entry_delay_seconds = entry_delay_seconds
        self._update_fn_or_price_key = update_fn_or_price_key
        self._market_entry_fn = market_entry_fn
        self._initial_entry_time = entry_time
        self._force_market_entry_seconds = force_market_entry_seconds
        self._expired_orders = []
        self._keep_expired_orders = keep_expired_orders


    def _on_step(self, item: dict):
        assert self._position is not None, "Missing ``position``"
        assert self.entry_time is not None, "Missing ``entry_time``"

        if self._losscut_price is not None:
            if check_stop(item, self.side, self._losscut_price):
                self._losscut(item)
                return

        if item['timestamp'] <= self.entry_time:
            # entry wait中
            return

        # 約定確認
        is_executed = self._check_execution(item)

        # 成行注文時に判定用関数が与えられている場合上書きする。主に``is_executed``をFalseに書き換える。（i.e., 待機時間は終了し
        # ていても条件を"満たさなければ"執行しない）。
        if is_executed and self.exec_type == ExecutionType.MARKET and self._market_entry_fn is not None:
            is_executed = self._market_entry_fn(item, self)

        if is_executed:
            self._executed(item)

        else:
            # (1) ``force_market_entry_seconds``が与えられた場合
            # (2) ``market_entry_fn``・LIMIT注文の場合
            if self.__need_force_market_entry(item):
                self._executed(item, force_market=True)

            else:
                # 約定しなかった場合、失効の有無を確認
                if self._check_expiration(item):
                    if self.exec_type == ExecutionType.MARKET:
                        # 成行注文は失効時に執行
                        # ``market_entry_fn``がNoneの場合：n秒後に必ず決済するロジック
                        # ``market_entry_fn``がNoneでない場合：決済タイミングが来ればn秒以内に成行決済するロジック
                        self._expired_and_executed(item)
                    else:
                        # 指値注文の場合は失効時に更新
                        is_updated = self._update(item)

    def cancel(self):
        super().cancel()
        self._position.clear_closing_order()

    def _executed(self, item, *, force_market=False, market_price_key=None):
        super()._executed(item, force_market=force_market, market_price_key=market_price_key)
        self._position.close(item, self)
        self._position.clear_closing_order()

    def _expired(self, item: dict):
        super()._expired(item)
        self._position.clear_closing_order()

    def _losscut(self, item: dict):
        market_price_key = "high" if self.side == Side.BUY else "low"
        self._executed(item, force_market=True, market_price_key=market_price_key)
        self._status = OrderStatus.LOSSCUT

    def _expired_and_executed(self, item: dict, market_price_key=None):
        self._expired(item)
        self._executed(item, force_market=True, market_price_key=market_price_key)
        self._status = OrderStatus.EXPIRED_EXECUTED

    def execution_time(self, metric='seconds'):
        if self.is_executed:
            return elapsed_time(
                self._initial_entry_time, self.executed_at, metric
            )
        else:
            return None

    @property
    def position(self):
        return self._position

    @property
    def expired_orders(self):
        return self._expired_orders

    def _update(self, item: dict):
        # 失効
        if self._update_fn_or_price_key is None:
            self._expired(item)
            return False

        assert not self.is_executed, "Already executed"

        # 指値変更
        # 失効注文を記録
        if self._keep_expired_orders:
            expired_order = copy.deepcopy(self)
            expired_order._expired(item)
            self._expired_orders.append(expired_order)

        # ``price``をアップデート
        if isinstance(self._update_fn_or_price_key, str):
            self.price = item[self._update_fn_or_price_key]
        else:
            self.price = self._update_fn_or_price_key(item, self)

        # ``entry_time``と``expire_time``の更新
        # 失効時のitemを削除
        self._expired_item = None

        # ``entry_time``を失効タイミングで上書きして、さらに失効タイミングを延長
        self._set_entry_and_expire_time(item['timestamp'])

        self._status = OrderStatus.ORDERING

        logger.debug(f"[EXTEND ORDER] {self}")

    def __need_force_market_entry(self, item):
        if (item['timestamp'] - self._initial_entry_time).seconds > self._force_market_entry_seconds:
            return True
        elif self.exec_type == ExecutionType.LIMIT and self._market_entry_fn is not None and self._market_entry_fn(item, self):
            return True
        else:
            return False
