import logging
import pandas as pd

# 求まるexpire_timeがテストデータに収まらないくらい先になるように十分大きい値、かつ、
# ``pd.to_datetime``が受け付ける値である必要がある
DEFAULT_EXPIRE_SECONDS = 1e+8

LOGGER_NAME = "botbacktester"
LOGGER_FMT = "[%(levelname)-6s %(asctime)s %(name)s] %(message)s"


def get_logger(level=logging.INFO):
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    hdlr = logging.StreamHandler()
    hdlr.setFormatter(logging.Formatter(LOGGER_FMT))
    logger.addHandler(hdlr)
    return logger


def set_log_level(level):
    logging.getLogger(LOGGER_NAME).setLevel(level)


def resample_candle(df, minute, key=None):
    assert df.index.name == 'timestamp'

    interval_sec = 60 * minute
    df_reset = df.reset_index()

    df_reset['timestamp'] = df_reset['timestamp'].dt.floor('{}S'.format(interval_sec))
    df_reset.index.name = None
    return pd.concat([
        df_reset.groupby('timestamp')[key or 'open'].nth(0).rename('open'),
        df_reset.groupby('timestamp')[key or 'high'].max().rename('high'),
        df_reset.groupby('timestamp')[key or 'low'].min().rename('low'),
        df_reset.groupby('timestamp')[key or 'close'].nth(-1).rename('close'),
        df_reset.groupby('timestamp')['volume'].sum().rename('volume'),
    ], axis=1)
