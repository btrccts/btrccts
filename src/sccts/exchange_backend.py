from ccxt.base.errors import InsufficientFunds
from collections import defaultdict
from decimal import Decimal


def _convert_float(f):
    return Decimal(str(f))


class Balance:

    def __init__(self, start_balance=0):
        self._total = _convert_float(start_balance)
        self._used = Decimal('0')
        if self._total < 0:
            raise ValueError('Balance: inital value cant be negative')

    def to_dict(self):
        return {
            'free': self.free(),
            'used': self.used(),
            'total': self.total(),
        }

    def free(self):
        return self._total - self._used

    def used(self):
        return self._used

    def total(self):
        return self._total

    def change_total(self, change):
        change = _convert_float(change)
        new_value = self._total + change
        if new_value < Decimal('0'):
            raise InsufficientFunds('Balance too little')
        self._total = new_value


class ExchangeBackend:

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._timeframe = timeframe
        self._start_balances = defaultdict(Balance)
        for key in balances:
            self._start_balances[key] = Balance(balances[key])
        self._balances = self._start_balances.copy()
        self._ohlcvs = ohlcvs
