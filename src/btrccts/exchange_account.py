import pandas
from ccxt.base.exchange import Exchange
from ccxt.base.errors import BadRequest, InvalidOrder, OrderNotFound
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal
from btrccts.check_dataframe import _check_dataframe
from btrccts.convert_float import _convert_float_or_raise, _convert_float
from btrccts.balance import Balance

DECIMAL_ONE = Decimal('1')


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
            fee_percentage = private_order['fee_percentage']

            self._remove_used_balance(price, amount, base, quote, buy)
            self._update_balance(price, amount, base, quote, buy,
                                 fee_percentage)
            self._fill_order(order, buy, price, timestamp, fee_percentage)
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
            self._remove_used_balance(amount=open_order['amount'],
                                      price=private['price'],
                                      base=private['base'],
                                      quote=private['quote'],
                                      buy=private['buy'])
            self._move_to_closed_orders(id)
            if private == self._next_private_order_to_update:
                self._update_next_private_order_to_update()
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
            'fee': {'currency': base if buy else quote,
                    'cost': None,
                    'rate': None},
            'trades': None,
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
            fee_percentage = market.get('taker', 0)
            fee_percentage = _convert_float_or_raise(fee_percentage,
                                                     'ExchangeAccount: fee')
            self._update_balance(price, amount, base, quote, buy,
                                 fee_percentage)
            self._fill_order(order, buy, price, timestamp, fee_percentage)
            self._closed_orders[order_id] = order
        if type_limit:
            # TODO Probably use taker fee, if the order can be filled now
            fee_percentage = market.get('maker', 0)
            fee_percentage = _convert_float_or_raise(fee_percentage,
                                                     'ExchangeAccount: fee')
            if buy:
                self._balances[quote].change_used(price * amount)
            else:
                self._balances[base].change_used(amount)
            self._open_orders[order_id] = order
            self._private_order_info[order_id] = {
                'id': order_id,
                'base': base,
                'quote': quote,
                'price': price,
                'buy': buy,
                'fee_percentage': fee_percentage,
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

    def _update_balance(self, price, amount, base, quote, buy, fee_percentage):
        # First decrease balance, then increase, so
        # decrease can throw and increase wont be affected
        multiplier = DECIMAL_ONE - fee_percentage
        if buy:
            self._balances[quote].change_total(- price * amount)
            self._balances[base].change_total(amount * multiplier)
        else:
            self._balances[base].change_total(- amount)
            self._balances[quote].change_total(price * amount * multiplier)

    def _remove_used_balance(self, price, amount, base, quote, buy):
        if buy:
            self._balances[quote].change_used(- price * amount)
        else:
            self._balances[base].change_used(- amount)

    def _fill_order(self, order, buy, price, timestamp, fee_percentage):
        amount = order['amount']
        amount_price = amount * price
        order.update({
            'average': price,
            'cost': amount_price,
            'filled': amount,
            'lastTradeTimestamp': timestamp,
            'price': price,
            'remaining': 0,
            'status': 'closed',
        })
        order['fee'].update({
            'rate': fee_percentage,
            'cost': fee_percentage * (amount if buy else amount_price),
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
        return self._return_decimal_to_float(deepcopy(order))

    def _filter_sort_orders(
            self, orders, since, limit, symbol, since_get, filter_non_zero):
        usable_orders = [order for _, order in orders.items()
                         if ((symbol is None or order['symbol'] == symbol) and
                             (filter_non_zero is None or
                              order[filter_non_zero] != 0) and
                             (since is None or order[since_get] > since))]
        usable_orders = sorted(usable_orders, key=lambda x: x[since_get])
        return usable_orders[:limit]

    def fetch_closed_orders(self, symbol=None, since=None, limit=None):
        self._update_orders()
        orders = self._filter_sort_orders(orders=self._closed_orders,
                                          symbol=symbol, limit=limit,
                                          since=since,
                                          filter_non_zero='filled',
                                          since_get='lastTradeTimestamp')
        return [self._return_decimal_to_float(deepcopy(o)) for o in orders]

    def fetch_open_orders(self, symbol=None, since=None, limit=None):
        self._update_orders()
        orders = self._filter_sort_orders(orders=self._open_orders,
                                          symbol=symbol, limit=limit,
                                          since=since,
                                          filter_non_zero=None,
                                          since_get='timestamp')
        return [self._return_decimal_to_float(deepcopy(o)) for o in orders]
