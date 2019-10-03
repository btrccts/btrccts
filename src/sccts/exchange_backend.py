import numpy
import pandas
from ccxt.base.exchange import Exchange
from ccxt.base.errors import BadRequest, InsufficientFunds, InvalidOrder, \
    OrderNotFound, BadSymbol
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
        if new_value + self._total < Decimal('0'):
            raise InsufficientFunds('Balance too little')
        self._used = new_value


def _check_dataframe(ohlcvs, timeframe, needed_columns=['low', 'high']):
    index = ohlcvs.index
    if index[0] > timeframe.start_date() or index[-1] < timeframe.end_date():
        raise ValueError('ohlcv needs to cover timeframe')
    for col in needed_columns:
        if col not in ohlcvs.columns:
            raise ValueError('ohlcv {} needs to be provided'.format(col))
    try:
        ohlcvs.index.freq = '1T'
    except ValueError:
        raise ValueError('ohlcv needs to be in 1T format')
    try:
        result = ohlcvs.astype(numpy.float)
        if not numpy.isfinite(result).values.all():
            raise ValueError('ohlcv needs to finite')
    except ValueError as e:
        raise ValueError('ohlcv {}'.format(str(e)))
    return result


class ExchangeAccount:

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._timeframe = timeframe
        self._start_balances = defaultdict(Balance)
        for key in balances:
            self._start_balances[key] = Balance(balances[key])
        self._balances = self._start_balances.copy()
        self._ohlcvs = {}
        for key in ohlcvs:
            self._ohlcvs[key] = _check_dataframe(ohlcvs[key], timeframe)
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
            raise OrderNotFound('ExchangeAccount: order {} does not exist'
                                .format(id))
        else:
            raise BadRequest('ExchangeAccount: cannot cancel market order')

    def create_order(self, market, type, price, side, amount):
        # Check parameters
        if type == 'market':
            if price is not None:
                raise InvalidOrder(
                    'ExchangeAccount: market order has no price')
        else:
            raise InvalidOrder('ExchangeAccount: only market order supported')
        if market is None:
            raise InvalidOrder('ExchangeAccount: market is None')
        symbol = market.get('symbol')
        if self._ohlcvs.get(symbol) is None:
            raise InvalidOrder('ExchangeAccount: no prices available for {}'
                               .format(symbol))
        if side not in ['buy', 'sell']:
            raise InvalidOrder('ExchangeAccount: side {} not supported'
                               .format(side))
        buy = side == 'buy'
        amount = _convert_float_or_raise(amount, 'ExchangeAccount: amount')
        if amount <= 0:
            raise BadRequest('ExchangeAccount: amount needs to be positive')
        base = market.get('base')
        quote = market.get('quote')
        if base is None:
            raise BadRequest('ExchangeAccount: market has no base')
        if quote is None:
            raise BadRequest('ExchangeAccount: market has no quote')

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
            raise OrderNotFound('ExchangeAccount: order {} does not exist'
                                .format(id))
        return self._return_decimal_to_float(order.copy())


class ExchangeBackend:

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._account = ExchangeAccount(timeframe=timeframe,
                                        balances=balances,
                                        ohlcvs=ohlcvs)
        self._ohlcvs = {}
        self._timeframe = timeframe
        for key in ohlcvs:
            self._ohlcvs[key] = _check_dataframe(
                ohlcvs[key],
                timeframe,
                ['open', 'low', 'high', 'close', 'volume'])

    def fetch_order(self, id, symbol=None):
        return self._account.fetch_order(id=id, symbol=symbol)

    def fetch_balance(self):
        return self._account.fetch_balance()

    def create_order(self, market, type, price, side, amount):
        return self._account.create_order(market=market, type=type, side=side,
                                          price=price, amount=amount)

    def cancel_order(self, id, symbol=None):
        return self._account.cancel_order(id=id, symbol=symbol)

    def fetch_ohlcv_dataframe(self, symbol, timeframe='1m', since=None,
                              limit=None, params={}):
        # Exchanges in the real world have different behaviour, when there is
        # no since parameter provided. (some use data from the beginning,
        # some from the end)
        # We return data from the beginning, because this is most likely not
        # what the user wants, so this will force the user to provide the
        # parameters, which will work with every exchange. This is a bug
        # prevention mechanism.
        ohlcv = self._ohlcvs.get(symbol)
        if ohlcv is None:
            raise BadSymbol('ExchangeBackend: no prices for {}'.format(symbol))
        current_date = self._timeframe.date().floor('1T')
        if limit is None:
            limit = 5
        timeframe_sec = Exchange.parse_timeframe(timeframe)
        pd_timeframe = pandas.Timedelta(timeframe_sec, unit='s')
        if since is None:
            pd_since = ohlcv.index[0]
        else:
            pd_since = pandas.Timestamp(since, unit='ms', tz='UTC')
        pd_since = pd_since.ceil(pd_timeframe)
        pd_until = pd_since + limit * pd_timeframe - pandas.Timedelta('1m')
        if pd_until > current_date:
            raise BadRequest(
                'ExchangeBackend: fetch_ohlcv:'
                ' since.ceil(timeframe) + limit * timeframe'
                ' needs to be in the past')
        data = ohlcv[pd_since:pd_until]
        return data.resample(pd_timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'})
