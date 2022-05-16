from enum import Enum, auto


class Side(Enum):
    BUY = auto()
    SELL = auto()

    def reverse(self):
        if self == Side.BUY:
            return Side.SELL
        else:
            return Side.BUY


class ExecutionType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP = auto()


class SettleType(Enum):
    OPEN = auto()
    CLOSE = auto()


class OrderStatus(Enum):
    ORDERING = auto()
    EXECUTED = auto()
    EXPIRED = auto()
    CANCELED = auto()
    EXPIRED_EXECUTED = auto()
    LOSSCUT = auto()
