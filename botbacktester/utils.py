import logging

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
