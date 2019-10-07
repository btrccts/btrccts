from ccxt.base.errors import InsufficientFunds
from decimal import Decimal
from sccts.convert_float import _convert_float


class Balance:

    def __init__(self, start_balance=0):
        self._total = _convert_float(start_balance)
        self._used = Decimal('0')
        if self._total < 0:
            raise ValueError('Balance: inital value cant be negative')

    def to_dict(self):
        return {
            'free': self._total - self._used,
            'used': self._used,
            'total': self._total,
        }

    def change_total(self, change):
        change = _convert_float(change)
        new_value = self._total + change
        if new_value < Decimal('0'):
            raise InsufficientFunds('Balance too little')
        self._total = new_value

    def change_used(self, change):
        change = _convert_float(change)
        new_value = self._used + change
        if new_value > self._total:
            raise InsufficientFunds('Balance too little')
        self._used = new_value

    def __repr__(self):
        return str(self.to_dict())
