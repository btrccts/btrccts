from ccxt.base.exchange import Exchange
from ccxt.base.errors import BadRequest, InsufficientFunds, InvalidOrder, \
    OrderNotFound
from collections import defaultdict
from decimal import Decimal, InvalidOperation


def _convert_float_or_raise(f, msg):
    try:
        val = _convert_float(f)
    except InvalidOperation:
        raise BadRequest('{} needs to be a number'.format(msg))
    if not val.is_finite():
        raise BadRequest('{} needs to be finite'.format(msg))
    return val


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


def _check_dataframe(ohlcvs, timeframe):
    index = ohlcvs.index
    if index[0] > timeframe.start_date() or index[-1] < timeframe.end_date():
        raise ValueError('ohlcv needs to cover timeframe')


class ExchangeBackend:

    # TODO: check that provided ohlcvs
    # - is parsable to Decimal
    # - low/high provided
    # - ohlcvs: is finite
    # - is 1m?
    # date +1m avail?
    # test that parameter can be modified afterwards

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._timeframe = timeframe
        self._start_balances = defaultdict(Balance)
        for key in balances:
            self._start_balances[key] = Balance(balances[key])
        self._balances = self._start_balances.copy()
        self._ohlcvs = {}
        for key in ohlcvs:
            ohlcv = ohlcvs[key]
            _check_dataframe(ohlcv, timeframe)
            self._ohlcvs[key] = ohlcv
        self._orders = {}
        self._last_order_id = 0

    def _return_decimal_to_float(self, result):
        for key in result.keys():
            value_type = type(result[key])
            if value_type == Decimal:
                result[key] = float(str(result[key]))
            elif value_type == dict:
                result[key] = self._return_decimal_to_float(result[key])
        return result

    def cancel_order(self, id, symbol=None):
        order = self._orders.get(id)
        if order is None:
            raise OrderNotFound('ExchangeBackend: order {} does not exist'
                                .format(id))
        else:
            raise BadRequest('ExchangeBackend: cannot cancel market order')

    def create_order(self, market, type, price, side, amount):
        # Check parameters
        if type == 'market':
            if price is not None:
                raise InvalidOrder(
                    'ExchangeBackend: market order has no price')
        else:
            raise InvalidOrder('ExchangeBackend: only market order supported')
        if market is None:
            raise InvalidOrder('ExchangeBackend: market is None')
        symbol = market.get('symbol')
        if self._ohlcvs.get(symbol) is None:
            raise InvalidOrder('ExchangeBackend: no prices available for {}'
                               .format(symbol))
        if side not in ['buy', 'sell']:
            raise InvalidOrder('ExchangeBackend: side {} not supported'
                               .format(side))
        buy = side == 'buy'
        amount = _convert_float_or_raise(amount, 'ExchangeBackend: amount')
        if amount <= 0:
            raise BadRequest('ExchangeBackend: amount needs to be positive')
        base = market.get('base')
        quote = market.get('quote')
        if base is None:
            raise BadRequest('ExchangeBackend: market has no base')
        if quote is None:
            raise BadRequest('ExchangeBackend: market has no quote')

        date = self._timeframe.date()

        # Determinie the price of the market order
        # We could use the next low/high to fill the order, but then we
        # need to wait for the next date to fill the order, otherwise we would
        # introduce a possibility to see the future price (Look-Ahead Bias)
        # If we wait for the next date, we would return a market order that is
        # pending, but this should never happen in reality
        # Maybe the factor should depend on the volume
        factor = Decimal('0.0015')
        if buy:
            price = (1 + factor) * _convert_float(
                self._ohlcvs.get(symbol)['high'][date])
            base_change = amount
            quote_change = - price * amount
            # First decrease balance, then increase, so
            # decrease can throw and increase wont be affected
            self._balances[quote].change_total(quote_change)
            self._balances[base].change_total(base_change)
        else:
            price = (1 - factor) * _convert_float(
                self._ohlcvs.get(symbol)['low'][date])
            base_change = -amount
            quote_change = price * amount
            # First decrease balance, then increase, so
            # decrease can throw and increase wont be affected
            self._balances[base].change_total(base_change)
            self._balances[quote].change_total(quote_change)

        self._last_order_id += 1
        order_id = str(self._last_order_id)
        timestamp = int(date.value / 10e5)
        order = {
            'info': {},
            'id': order_id,
            'timestamp': timestamp,
            'datetime': Exchange.iso8601(timestamp),
            'lastTradeTimestamp': timestamp,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': price,
            'amount': amount,
            'cost': price * amount,
            'average': price,
            'filled': amount,
            'remaining': 0,
            'status': 'closed',
            'fee': 0,  # TODO
            'trades': None,
        }
        self._orders[order_id] = order
        return {'id': order_id,
                'info': {}}

    def fetch_balance(self):
        result = {}
        for key, balance in self._balances.items():
            result[key] = self._return_decimal_to_float(balance.to_dict())
        return result

    def fetch_order(self, id, symbol=None):
        order = self._orders.get(id)
        if order is None:
            raise OrderNotFound('ExchangeBackend: order {} does not exist'
                                .format(id))
        return self._return_decimal_to_float(order.copy())
