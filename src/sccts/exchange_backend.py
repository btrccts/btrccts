from collections import defaultdict
from decimal import Decimal


class Balance:

    def __init__(self, start_balance=0):
        self._total = Decimal(str(start_balance))
        self._used = Decimal('0')
        if self._total < 0:
            raise ValueError('Balance: inital value cant be negative')

    def free(self):
        return self._total - self._used

    def used(self):
        return self._used

    def total(self):
        return self._total


class ExchangeBackend:

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._timeframe = timeframe
        self._start_balances = defaultdict(Balance)
        for key in balances:
            self._start_balances[key] = Balance(balances[key])
        self._balances = self._start_balances.copy()
        self._ohlcvs = ohlcvs
