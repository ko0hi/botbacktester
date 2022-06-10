from typing import Optional

import numba
import numpy as np
import pandas as pd

from enum import IntEnum, auto


class Status(IntEnum):
    SUCCESS = auto()
    ENTRY_TIMEOUT = auto()
    EXIT_TIMEOUT = auto()
    FILTERED = auto()
    LOSSCUT = auto()


def limit_simulation(
    df: pd.DataFrame,
    side: int,
    *,
    entry_prices: Optional[np.ndarray] = None,
    exit_prices: Optional[np.ndarray] = None,
    losscut_prices: Optional[np.ndarray] = None,
    entry_filter: Optional[np.ndarray] = None,
    timelimit: Optional[int] = np.inf,
    timelimit_type: Optional[str] = "time",
    losscut_slippage: Optional[int] = 2000,
) -> pd.DataFrame:
    """指値注文のバックテスト。

    :param df:　ohlcv
    :param side: 1 ("BUY") or -1 ("SELL")
    :param entry_prices: エントリー指値
    :param exit_prices:　エグジット指値
    :param losscut_prices:　ストップ指値
    :param entry_filter: エントリーの可否 1 (OK) or 0 (NG)
    :param timelimit: キャンセルまでのリードタイム or バーの本数
    :param timelimit_type:　timelimitのタイプ（"time" or "bar"）
    :param losscut_slippage: ロスカット時のスリップ幅
    :return:

    - entry_prices・exit_prices未指定の場合、dfは"buy_price"と"sell_price"カラムを持っていなければならない

    """

    @numba.jit(nopython=True)
    def _calc(
        entry_prices,
        exit_prices,
        losscut_prices,
        open_prices,
        high_prices,
        low_prices,
        close_prices,
        timestamps,
        timelimit,
        side,
        losscut_slippage,
        entry_filter,
        timelimit_type,
    ):
        N = len(entry_prices)

        entry_at = np.full(N, np.nan)
        exit_at = np.full(N, np.nan)
        entry_price = np.full(N, np.nan)
        exit_price = np.full(N, np.nan)
        status = np.full(N, 1)
        __entry_at_bar = np.full(N, np.nan)

        if timelimit_type == "time":
            unix_seconds = 1000000000
            timelimit = timelimit * unix_seconds

        has_losscut = ~np.isnan(losscut_prices).all()

        for i in range(N):
            if entry_filter[i] == 0:
                status[i] = Status.FILTERED
                continue

            i_ts = timestamps[i]
            for j in range(i + 1, N):
                j_ts = timestamps[j]
                on_entry = np.isnan(entry_at[i])

                # 時刻を過ぎているかの判定
                if timelimit_type == "time":
                    elapsed = j_ts - i_ts if on_entry else j_ts - entry_at[i]
                else:
                    elapsed = j - i if on_entry else j - __entry_at_bar[i]

                tl = timelimit[0] if on_entry else timelimit[1]

                if elapsed > tl:
                    # exitできている場合は下でbreakされる
                    assert np.isnan(exit_at[i])

                    # entryはしたがexitできてない場合は現時刻のcloseでexitする
                    if ~on_entry:
                        exit_at[i] = j_ts
                        exit_price[i] = close_prices[j]
                        status[i] = Status.EXIT_TIMEOUT

                    break

                # Long
                if side == 1:
                    # Entry
                    if on_entry:
                        if low_prices[j] < entry_prices[i]:
                            entry_at[i] = j_ts
                            __entry_at_bar[i] = j
                            entry_price[i] = entry_prices[i]
                    # Exit
                    else:
                        if high_prices[j] > exit_prices[i]:
                            exit_at[i] = j_ts
                            exit_price[i] = exit_prices[i]
                            status[i] = Status.SUCCESS

                        if has_losscut and low_prices[j] < losscut_prices[i]:
                            exit_at[i] = j_ts
                            exit_price[i] = losscut_prices[i] - losscut_slippage
                            status[i] = Status.LOSSCUT

                        if ~np.isnan(exit_at[i]):
                            break

                # Short
                elif side == -1:
                    # Entry
                    if on_entry:
                        if high_prices[j] > entry_prices[i]:
                            entry_at[i] = j_ts
                            __entry_at_bar[i] = j
                            entry_price[i] = entry_prices[i]
                    # Exit
                    else:
                        if low_prices[j] < exit_prices[i]:
                            exit_at[i] = j_ts
                            exit_price[i] = exit_prices[i]
                            status[i] = Status.SUCCESS

                        if has_losscut and high_prices[j] > losscut_prices[i]:
                            exit_at[i] = j_ts
                            exit_price[i] = losscut_prices[i] + losscut_slippage
                            status[i] = Status.LOSSCUT

                        if ~np.isnan(exit_at[i]):
                            break

        return entry_at, entry_price, exit_at, exit_price, status

    # check
    assert df.index.name == "timestamp"
    assert isinstance(df.index, pd.DatetimeIndex)
    # if df.index.tz is None:
    #     df.index = pd.to_datetime(df.index, utc=True)
    assert side in [1, -1]
    assert all([c in df.columns for c in ["open", "high", "low", "close"]])
    assert timelimit_type in ["time", "bar"]

    def _to_numpy(p):
        if isinstance(p, str):
            return df[p].values
        elif isinstance(p, pd.Series):
            return p.values
        elif isinstance(p, np.ndarray):
            return p
        else:
            raise RuntimeError

    if entry_prices is None:
        entry_prices = "buy_price" if side == 1 else "sell_price"
    entry_prices = _to_numpy(entry_prices)

    if exit_prices is None:
        exit_prices = "sell_price" if side == 1 else "buy_price"
    exit_prices = _to_numpy(exit_prices)

    if losscut_prices is None:
        losscut_prices = np.full(len(df), np.nan)
    else:
        losscut_prices = _to_numpy(losscut_prices)

    if entry_filter is None:
        entry_filter = np.ones(len(df))
    else:
        entry_filter = _to_numpy(entry_filter)
    entry_filter = entry_filter.astype(int)

    if isinstance(timelimit, (list, tuple)):
        assert len(timelimit) == 2
        timelimit = np.array(timelimit)
    else:
        timelimit = np.array([timelimit, timelimit])

    values = _calc(
        entry_prices,
        exit_prices,
        losscut_prices,
        df.open.values,
        df.high.values,
        df.low.values,
        df.close.values,
        df.index.values.astype(int),
        timelimit,
        side,
        losscut_slippage,
        entry_filter,
        timelimit_type,
    )

    df_ = pd.DataFrame(list(values) + [entry_prices, exit_prices, losscut_prices]).T
    df_.index = df.index
    df_.columns = [
        "entry_at",
        "entry_price",
        "exit_at",
        "exit_price",
        "status",
        "entry_price_order",
        "exit_price_order",
        "losscut_price_order",
    ]
    for c in df_.columns:
        if c.endswith("at"):
            df_[c] = pd.to_datetime(df_[c], utc=True)

    df_["status"] = df_["status"].astype(int)

    if side == 1:
        df_["profit"] = df_.exit_price / df_.entry_price - 1
    else:
        df_["profit"] = (df_.exit_price / df_.entry_price - 1) * -1

    df_["id"] = np.arange(len(df_))

    df_["is_win"] = np.where(df_.entry_at.isna(), np.nan, df_.profit > 0)
    df_["entry_duration"] = (df_.entry_at - df_.index).dt.total_seconds()
    df_["exit_duration"] = (df_.exit_at - df_.entry_at).dt.total_seconds()
    df_["total_duration"] = (df_.exit_at - df_.index).dt.total_seconds()

    return df_
