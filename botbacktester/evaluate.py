"""　シミュレーション結果評価関数。``df``は``BackTester.get_result_df()``によって与えられるもの。

"""

import numpy as np
import matplotlib.pyplot as plt

TIME_UNITS = {"H": 3600, "M": 60, "S": 1}


def drawdown(df, name="drawdown"):
    assert 'gain' in df.columns
    s = df.gain.cumsum() - df.gain.cumsum().cummax()
    s = np.minimum(s, 0)
    s.name = name
    return s


def win_ratio(df, n=100, name="win_ratio"):
    assert 'gain' in df.columns
    s = (df.gain > 0).rolling(n).mean()
    s.name = name
    return s


def ls_ratio(df, n=100, name="ls_ratio"):
    assert 'side' in df.columns
    s = pd.Series(np.where(df.side == "BUY", 1, 0)).rolling(n).mean()
    s.name = name
    return s


def position_frequency(df, n=100, name="position_frequency", time_unit='S'):
    df_tmp = df.reset_index()
    seconds = (df_tmp.shift(-n).timestamp - df_tmp.timestamp).dt.total_seconds()
    seconds = seconds / n / TIME_UNITS[time_unit]
    seconds.index = df.index
    seconds.name = name
    return seconds


def position_term(df, n=100, name="position_term", time_unit='S'):
    assert 'oo_executed_at' in df.columns
    assert 'co_executed_at' in df.columns
    s = (df.co_executed_at - df.oo_executed_at).dt.total_seconds() / TIME_UNITS[time_unit]
    s = s.rolling(n).mean()
    s.name = name
    return s


def execution_time(df, settle_type, n=100, name="execution_time", time_unit='S'):
    prefix = 'oo' if settle_type.lower() == 'open' else 'co'
    s = (df[f"{prefix}_executed_at"] - df[f"oo_entried_at"]).dt.total_seconds() / TIME_UNITS[time_unit]
    s = s.rolling(n).mean()
    s.name = name
    return s


def report(df, n=100, time_unit='S', figsize=(10, 10), hspace=None, subplots_kw=None, subpanel_size_ratio=0.1, ax_kw_dict=None):
    ax_kw_dict = ax_kw_dict or {}
    if subplots_kw is None:
        subplots_kw = dict(
            gridspec_kw=dict(height_ratios=[1] + [subpanel_size_ratio] * 4),
        )

    subplots_kw['sharex'] = True
    subplots_kw['figsize'] = figsize
    subplots_kw['gridspec_kw']['hspace'] = hspace

    fig, axes = plt.subplots(5, 1, **subplots_kw)

    ax_iter = iter(axes)
    ax = next(ax_iter)
    df.gain.cumsum().plot(ax=ax, **ax_kw_dict.get('cumgain', {}))
    ax.set_title("Cumulative reward")

    ax = next(ax_iter)
    drawdown(df).plot(ax=ax, **ax_kw_dict.get('drawdown', {}))
    ax.set_title("DD")

    ax = next(ax_iter)
    win_ratio(df, n=n).plot(ax=ax, **ax_kw_dict.get('win_ratio', {}))
    ax.set_title("Win ratio")
    ax.set_ylim((0, 1))

    ax = next(ax_iter)
    position_frequency(df, n=n, time_unit=time_unit).plot(ax=ax, **ax_kw_dict.get('position_frequency', {}))
    ax.set_title("Position frequency")

    ax = next(ax_iter)
    position_term(df, n=n, time_unit=time_unit).plot(ax=ax, **ax_kw_dict.get('position_term', {}))
    ax.set_title("Position term")

    for ax_ in axes:
        ax_.grid()
