import pandas
from ccxt.base.exchange import Exchange
from ccxt.base.errors import BadRequest, InvalidOrder, OrderNotFound
from collections import defaultdict
from decimal import Decimal
from sccts.check_dataframe import _check_dataframe
from sccts.convert_float import _convert_float_or_raise, _convert_float
from sccts.balance import Balance


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
        self._last_order_id = 0
        self._open_orders = {}
        self._closed_orders = {}
        self._private_order_info = {}
        self._next_private_order_to_update = None

    def _move_to_closed_orders(self, id):
        self._closed_orders[id] = self._open_orders[id]
        del self._open_orders[id]
        del self._private_order_info[id]

    def _update_next_private_order_to_update(self):
        try:
            self._next_private_order_to_update = min(
                filter(lambda x: x['fillable_date'] is not None,
                       self._private_order_info.values()),
                key=lambda x: x['fillable_date'])
        except ValueError:
            self._next_private_order_to_update = None

    def _update_orders(self):
        curr_date = self._timeframe.date()
        while True:
            private_order = self._next_private_order_to_update
            if private_order is None:
                return
            fillable_date = private_order['fillable_date']
            if fillable_date > curr_date:
                return
            order_id = private_order['id']
            timestamp = int(fillable_date.value / 10e5)
            order = self._open_orders[order_id]

            amount = order['amount']
            price = private_order['price']
            base = private_order['base']
            quote = private_order['quote']
            buy = private_order['buy']

            self._remove_used(price, amount, base, quote, buy)
            self._update_balance(price, amount, base, quote, buy)
            self._fill_order(order, price, timestamp)
            self._move_to_closed_orders(order_id)

            self._update_next_private_order_to_update()

    def _return_decimal_to_float(self, result):
        for key in result.keys():
            value_type = type(result[key])
            if value_type == Decimal:
                result[key] = float(str(result[key]))
            elif value_type == dict:
                result[key] = self._return_decimal_to_float(result[key])
        return result

    def cancel_order(self, id, symbol=None):
        self._update_orders()
        closed_order = self._closed_orders.get(id)
        if closed_order is not None:
            raise BadRequest('ExchangeAccount: cannot cancel {} order {}'
                             .format(closed_order['status'], id))
        open_order = self._open_orders.get(id)
        if open_order is None:
            raise OrderNotFound('ExchangeAccount: order {} does not exist'
                                .format(id))
        else:
            open_order.update({
                'status': 'canceled',
            })
            private = self._private_order_info[id]
            self._remove_used(amount=open_order['amount'],
                              price=private['price'],
                              base=private['base'],
                              quote=private['quote'],
                              buy=private['buy'])
            self._move_to_closed_orders(id)
            return {'id': id,
                    'info': {}}

    def create_order(self, market, type, price, side, amount):
        self._update_orders()
        type_market = False
        type_limit = False
        if type == 'market':
            if price is not None:
                raise InvalidOrder(
                    'ExchangeAccount: market order has no price')
            type_market = True
        elif type == 'limit':
            price = _convert_float_or_raise(price, 'ExchangeAccount: price')
            type_limit = True
            if price <= 0:
                raise BadRequest('ExchangeAccount: price needs to be positive')
        else:
            raise InvalidOrder(
                'ExchangeAccount: only market and limit order supported')
        if market is None:
            raise InvalidOrder('ExchangeAccount: market is None')
        symbol = market.get('symbol')
        ohlcv = self._ohlcvs.get(symbol)
        if ohlcv is None:
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

        self._last_order_id += 1
        order_id = str(self._last_order_id)
        date = self._timeframe.date()
        timestamp = int(date.value / 10e5)
        order = {
            'info': {},
            'id': order_id,
            'timestamp': timestamp,
            'datetime': Exchange.iso8601(timestamp),
            'lastTradeTimestamp': None,
            'symbol': symbol,
            'type': type,
            'side': side,
            'price': None,
            'amount': amount,
            'cost': None,
            'average': None,
            'filled': 0,
            'remaining': amount,
            'status': 'open',
            'fee': 0,  # TODO {'currency': 'BTC', 'cost': 0, 'rate': 0}
            'trades': None,  # TODO: []
        }

        if type_market:
            # Determinie the price of the market order
            # We could use the next low/high to fill the order, but then we
            # need to wait for the next date to fill the order, otherwise we
            # would introduce a possibility to see the future price
            # (Look-Ahead Bias)
            # If we wait for the next date, we would return a market order that
            # is pending, but this should never happen in reality
            # Maybe the factor should depend on the volume
            factor = Decimal('0.0015')
            if buy:
                price = (1 + factor) * _convert_float(ohlcv['high'][date])
            else:
                price = (1 - factor) * _convert_float(ohlcv['low'][date])
            self._update_balance(price, amount, base, quote, buy)
            self._fill_order(order, price, timestamp)
            self._closed_orders[order_id] = order
        if type_limit:
            self._open_orders[order_id] = order
            if buy:
                self._balances[quote].change_used(price * amount)
            else:
                self._balances[base].change_used(amount)
            self._private_order_info[order_id] = {
                'id': order_id,
                'base': base,
                'quote': quote,
                'price': price,
                'buy': buy,
                'fillable_date': self._limit_order_fillable_date(
                    symbol, buy, price),
            }
            self._update_next_private_order_to_update()

        return {'id': order_id,
                'info': {}}

    def _limit_order_fillable_date(self, symbol, buy, price):
        ohlcv = self._ohlcvs[symbol]
        date = self._timeframe.date()
        if ohlcv.index[0] != date:
            ohlcv = ohlcv[date:]
            # save reduced dataframe for better performance
            self._ohlcvs[symbol] = ohlcv
        # only look at the future
        ohlcv = ohlcv[date + pandas.Timedelta(1, unit='ns'):]
        if buy:
            low = ohlcv.low
            use = low[low <= price]
        else:
            high = ohlcv.high
            use = high[high >= price]
        if use is not None and len(use.index) > 0:
            return use.index[0]
        else:
            return None

    def _update_balance(self, price, amount, base, quote, buy):
        # First decrease balance, then increase, so
        # decrease can throw and increase wont be affected
        if buy:
            self._balances[quote].change_total(- price * amount)
            self._balances[base].change_total(amount)
        else:
            self._balances[base].change_total(- amount)
            self._balances[quote].change_total(price * amount)

    def _remove_used(self, price, amount, base, quote, buy):
        if buy:
            self._balances[quote].change_used(- price * amount)
        else:
            self._balances[base].change_used(- amount)

    def _fill_order(self, order, price, timestamp):
        order.update({
            'average': price,
            'cost': order['amount'] * price,
            'filled': order['amount'],
            'lastTradeTimestamp': timestamp,
            'price': price,
            'remaining': 0,
            'status': 'closed',
        })

    def fetch_balance(self):
        self._update_orders()
        result = {}
        for key, balance in self._balances.items():
            result[key] = self._return_decimal_to_float(balance.to_dict())
        return result

    def fetch_order(self, id, symbol=None):
        self._update_orders()
        order = self._closed_orders.get(id)
        if order is None:
            order = self._open_orders.get(id)
        if order is None:
            raise OrderNotFound('ExchangeAccount: order {} does not exist'
                                .format(id))
        return self._return_decimal_to_float(order.copy())
