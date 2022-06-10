import pandas as pd

from io import StringIO

import botbacktester as bbt
import botbacktester.enums as E


def _read_test_df():
    s = """timestamp,openTime,open,high,low,close,volume
    2021-04-16 20:59:00,1618606800000,6752008.0,6759609.0,6752008.0,6757588.0,11.1
    2021-04-16 21:00:00,1618606800000,6752008.0,6759609.0,6752008.0,6757588.0,11.1
    2021-04-16 21:01:00,1618606860000,6757000.0,6759079.0,6755500.0,6759079.0,3.26
    2021-04-16 21:02:00,1618606920000,6757821.0,6760000.0,6757821.0,6758775.0,0.24
    2021-04-16 21:03:00,1618606980000,6758781.0,6759464.0,6757563.0,6757605.0,0.62
    2021-04-16 21:04:00,1618607040000,6759533.0,6761779.0,6757705.0,6759531.0,1.44
    2021-04-16 21:05:00,1618607100000,6759553.0,6761779.0,6759553.0,6759848.0,1.52
    2021-04-16 21:06:00,1618607160000,6760847.0,6763590.0,6759788.0,6760665.0,1.92
    2021-04-16 21:07:00,1618607220000,6761188.0,6763263.0,6758350.0,6761029.0,2.08
    2021-04-16 21:08:00,1618607280000,6761029.0,6762841.0,6760508.0,6762841.0,0.64
    2021-04-16 21:09:00,1618607340000,6762860.0,6766377.0,6762860.0,6763890.0,2.06
    2021-04-16 21:10:00,1618607400000,6763890.0,6764382.0,6760790.0,6762843.0,1.82
    """
    df = pd.read_csv(StringIO(s))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    return df


def test_limit1():
    tester = bbt.BackTester(_read_test_df())
    entry_price = 6759000
    exit_price = 6763000
    for i, item in tester.start():

        orders = tester.orders()
        positions = tester.positions(non_closing=True)

        ts = item["timestamp"]

        if ts.minute < 5:
            assert len(orders) == 0
            assert len(positions) == 0
            assert tester.status.order_num == 0
            assert tester.status.position_num == 0
            assert tester.status.cum_gain == 0
            assert len(tester.position_history) == 0

        elif ts.minute == 5:
            # open entry
            assert len(orders) == 0
            assert len(positions) == 0
            assert tester.status.order_num == 0
            assert tester.status.position_num == 0
            assert tester.status.cum_gain == 0

            tester.entry(E.Side.BUY, E.ExecutionType.LIMIT, price=entry_price)

            orders_ = tester.orders()
            assert len(orders_) == 1
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0
            assert tester.status.cum_gain == 0
            assert len(tester.position_history) == 0

            o = orders_[0]
            assert o.entried_at.minute == 5
            assert o.status == E.OrderStatus.ORDERING
            assert not o.is_executed
            assert o.executed_item is None
            assert o.executed_at is None
            assert not o.is_expired
            assert o.expired_item is None
            assert o.expired_at is None
            assert not o.is_done

        elif ts.minute == 6:
            # open ordering
            assert len(orders) == 1
            assert len(positions) == 0
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0
            assert tester.status.cum_gain == 0
            assert len(tester.position_history) == 0

            o = orders[0]
            assert o.status == E.OrderStatus.ORDERING

        elif ts.minute == 7:
            # open executed
            # close entry

            assert len(orders) == 0
            assert len(positions) == 1
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1
            assert tester.status.cum_gain == 0
            assert len(tester.position_history) == 0

            p = positions[0]
            o = p.open_order

            assert not p.is_closing
            assert not p.is_closed
            assert p.open_price == o.price
            assert o.status == E.OrderStatus.EXECUTED
            assert o.is_executed
            assert o.executed_at.minute == 7
            assert str(o.executed_item["timestamp"]) == "2021-04-16 21:07:00"

            tester.exit(p, E.ExecutionType.LIMIT, price=exit_price)

        elif ts.minute == 8:
            # close ordering

            assert len(orders) == 1
            assert len(positions) == 0
            assert tester.status.order_num == 1
            assert tester.status.position_num == 1
            assert tester.status.cum_gain == 0
            assert len(tester.position_history) == 0

            positions_ = tester.positions()
            assert len(positions_) == 1

            o = orders[0]
            p = positions_[0]

            assert o.position.id == p.id
            assert o.entried_at.minute == 7
            assert o.status == E.OrderStatus.ORDERING

            assert p.is_closing
            assert not p.is_closed
            assert p.closing_order.id == o.id

        elif ts.minute == 9:
            # close executed

            assert len(orders) == 0
            assert len(positions) == 0
            assert tester.status.order_num == 0
            assert tester.status.position_num == 0
            assert tester.status.cum_gain == exit_price / entry_price - 1
            assert len(tester.position_history) == 1

            p = tester.position_history[0]

            assert p.open_order.entried_at.minute == 5
            assert p.open_order.executed_at.minute == 7
            assert p.open_order.status == E.OrderStatus.EXECUTED
            assert p.close_order.entried_at.minute == 7
            assert p.close_order.executed_at.minute == 9
            assert p.close_order.status == E.OrderStatus.EXECUTED
            assert p.gain == exit_price / entry_price - 1


def test_open_order_expire1():
    # n秒後に失効
    tester = bbt.BackTester(_read_test_df())

    for i, item in tester.start():
        ts = item["timestamp"]

        orders = tester.orders()

        if ts.minute == 3:
            o = tester.entry(
                E.Side.BUY,
                E.ExecutionType.LIMIT,
                price=-float("inf"),
                expire_seconds=180,
            )
            # entry timeを含むのでexpire_secondsの設定はそこを考慮して行う必要がある
            assert str(o.expire_time) == "2021-04-16 21:06:00"
            assert o.status == E.OrderStatus.ORDERING

        elif 4 <= ts.minute <= 5:
            assert len(orders) == 1
            assert len(tester.order_history) == 1

            o = orders[0]
            assert o.status == E.OrderStatus.ORDERING
            assert not o.is_expired

        elif ts.hour == 21 and ts.minute >= 6:
            # 失効時点時刻を過ぎた時点で失効（i.e., 21:06の足は対象外）
            # 失効したらstatusからは削除される
            assert len(orders) == 0

            # tester.order_historyに記録が残る
            assert len(tester.order_history) == 1

            o = tester.order_history[0]

            assert o.status == E.OrderStatus.EXPIRED
            assert o.is_expired
            assert str(o.expired_at) == "2021-04-16 21:06:00"
            assert not o.is_executed

            # 失効または約定でTrue
            assert o.is_done


def test_open_order_expire2():
    # 最後まで約定しない
    tester = bbt.BackTester(_read_test_df())

    for i, item in tester.start():
        ts = item["timestamp"]

        orders = tester.orders()

        if ts.minute == 3:
            # defaultは失効しない
            o = tester.entry(E.Side.BUY, E.ExecutionType.LIMIT, price=-float("inf"))
            assert o.status == E.OrderStatus.ORDERING

        elif ts.hour == 21 and ts.minute > 3:
            assert tester.status.order_num == 1
            assert not tester.status.orders()[0].is_expired

    # 最後まで約定しなかった場合、最終で強制失効する
    assert len(tester.order_history) == 1
    o = tester.order_history[0]
    assert o.status == E.OrderStatus.EXPIRED
    assert o.is_expired
    assert str(o.expired_at) == "2021-04-16 21:10:00"


def test_close_order_expire1():
    # CloseOrderが最後まで約定しない場合は最終価格で強制決済
    tester = bbt.BackTester(_read_test_df())
    entry_price = 6763000

    for i, item in tester.start():
        ts = item["timestamp"]

        if ts.minute == 4:
            o = tester.entry(E.Side.SELL, E.ExecutionType.LIMIT, price=entry_price)
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        if ts.minute == 5:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        elif ts.minute == 6:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1

            p = tester.positions(non_closing=True)[0]
            o = tester.exit(p, E.ExecutionType.LIMIT, price=-float("inf"))

        elif ts.hour == 21 and ts.minute > 7:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 1

            o = tester.orders()[0]
            assert o.status == E.OrderStatus.ORDERING

    assert tester.status.order_num == 0
    assert tester.status.position_num == 0

    # 残ったポジションは最終価格で強制決済
    assert len(tester.position_history) == 1
    p = tester.position_history[0]
    assert p.open_order.entried_at.minute == 4
    assert p.close_order.entried_at.minute == 6
    assert p.close_order.status == E.OrderStatus.EXPIRED_EXECUTED
    assert p.gain == (tester._data[-1]["close"] / entry_price - 1) * -1


def test_no_close_order_position1():
    # CloseOrderが出されないまま残ったポジションも最終価格で強制決済

    tester = bbt.BackTester(_read_test_df())
    entry_price = 6763000

    for i, item in tester.start():
        ts = item["timestamp"]

        if ts.minute == 4:
            o = tester.entry(E.Side.SELL, E.ExecutionType.LIMIT, price=entry_price)
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        if ts.minute == 5:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        elif ts.minute == 6:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1

        elif ts.hour == 21 and ts.minute > 7:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1

    assert tester.status.order_num == 0
    assert tester.status.position_num == 0
    assert len(tester.position_history) == 1
    p = tester.position_history[0]
    assert p.open_order.entried_at.minute == 4
    assert p.close_order.entried_at.minute == 10  # ここが違う
    assert p.close_order.status == E.OrderStatus.EXECUTED  # ここも違う
    assert p.gain == (tester._data[-1]["close"] / entry_price - 1) * -1


def test_no_close_order_position2():
    # CloseOrderは注文されたものの約定せずに失効した場合も、最後の価格で強制決済
    tester = bbt.BackTester(_read_test_df())
    entry_price = 6763000

    for i, item in tester.start():
        ts = item["timestamp"]

        if ts.minute == 4:
            o = tester.entry(E.Side.SELL, E.ExecutionType.LIMIT, price=entry_price)
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        if ts.minute == 5:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 0

        elif ts.minute == 6:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1

            p = tester.positions(non_closing=True)[0]
            o = tester.exit(
                p, E.ExecutionType.LIMIT, price=-float("inf"), expire_seconds=120
            )

        elif ts.minute == 7:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 1

            o = tester.orders()[0]
            assert o.status == E.OrderStatus.ORDERING

        elif ts.hour == 21 and ts.minute >= 8:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1
            o = tester.order_history[1]
            assert o.settle_type == E.SettleType.CLOSE
            assert o.is_expired
            assert o.expired_at.minute == 8

    assert tester.status.order_num == 0
    assert tester.status.position_num == 0
    assert len(tester.position_history) == 1
    p = tester.position_history[0]
    assert p.open_order.entried_at.minute == 4
    assert p.close_order.entried_at.minute == 10
    assert p.close_order.status == E.OrderStatus.EXECUTED
    assert p.gain == (tester._data[-1]["close"] / entry_price - 1) * -1


def test_market1():
    # Market entryは次時刻のopen価格でエントリーされる
    tester = bbt.BackTester(_read_test_df())

    slip = 1000

    for i, item in tester.start():
        if i == 5:
            o = tester.entry(E.Side.BUY, E.ExecutionType.MARKET)
            assert o.price == -1
            assert tester.status.position_num == 0
        elif i == 6:
            assert tester.status.position_num == 1
            p = tester.positions()[0]
            assert p.open_price == item["open"]

        elif i == 7:
            o = tester.entry(E.Side.BUY, E.ExecutionType.MARKET, market_slippage=slip)
            assert o.price == -1

        elif i == 8:
            assert tester.status.position_num == 2
            p = tester.positions()[1]
            assert p.open_price == item["open"] + slip


def test_losscut1():
    # losscut_priceを超えた・割ったタイミングで成行執行される

    tester = bbt.BackTester(_read_test_df())

    entry_price = None
    losscut_price = 6759000

    for i, item in tester.start():
        ts = item["timestamp"]
        if ts.minute == 4:
            o = tester.entry(E.Side.BUY, E.ExecutionType.MARKET)

        elif ts.minute == 5:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 1
            p = tester.positions()[0]
            o = tester.exit(
                p,
                E.ExecutionType.LIMIT,
                price=float("inf"),
                losscur_price=losscut_price,
            )
            entry_price = item["open"]

        elif ts.minute == 6:
            assert tester.status.order_num == 1
            assert tester.status.position_num == 1

        elif ts.minute == 7:
            assert tester.status.order_num == 0
            assert tester.status.position_num == 0

            assert len(tester.position_history) == 1

            p = tester.position_history[0]

            assert p.close_order.exec_type == E.ExecutionType.MARKET
            assert p.close_order.status == E.OrderStatus.LOSSCUT
            assert p.open_price == entry_price
            assert p.close_price == item["low"]
            assert p.gain == item["low"] / entry_price - 1
