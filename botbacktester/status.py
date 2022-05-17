from __future__ import annotations

from .items import Order, Position


class OrderSet:
    def __init__(self, open_order):
        self._open_order = open_order
        self._position = None
        self._close_order = None

    def set_position(self, p):
        self._position = p

    def set_close_order(self, co):
        self._close_order = co


class Status:
    def __init__(self):
        self._orders: list[Order] = []
        self._positions: list[Position] = []
        self._cum_gain: float = 0

    def add_order(self, o):
        self._orders.append(o)

    def add_position(self, p):
        self._positions.append(p)

    def remove_order(self, o):
        self._orders = [o_ for o_ in self._orders if id(o) != id(o_)]

    def remove_position(self, p):
        self._positions = [p_ for p_ in self._positions if id(p) != id(p_)]

    def clear_done_orders(self):
        remain_orders, done_orders = [], []
        for o in self._orders:
            if o.is_done:
                done_orders.append(o)
            else:
                remain_orders.append(o)

        self._orders = remain_orders

        return done_orders

    def clear_closed_positions(self):
        remain_positions, closed_positions = [], []

        for p in self._positions:
            if p.is_closed:
                closed_positions.append(p)
                self._cum_gain += p.gain
            else:
                remain_positions.append(p)

        self._positions = remain_positions

        return closed_positions

    def orders(self, side=None, settle_type=None, exec_type=None) -> list[Order]:
        rtn_orders = []
        for o in self._orders:
            if side and o.side != side:
                continue

            if settle_type and o.settle_type != settle_type:
                continue

            if exec_type and o.exec_type != exec_type:
                continue

            rtn_orders.append(o)

        return rtn_orders

    def positions(self, side=None, non_closing=False) -> list[Position]:
        rtn_positions = []

        for p in self._positions:
            if side and p.side != side:
                continue

            if non_closing and p.is_closing:
                continue

            rtn_positions.append(p)

        return rtn_positions

    @property
    def cum_gain(self):
        return self._cum_gain

    @property
    def order_num(self):
        return len(self._orders)

    @property
    def position_num(self):
        return len(self._positions)
