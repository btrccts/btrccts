import pandas
import unittest
from ccxt.base.errors import BadRequest, InsufficientFunds, InvalidOrder, \
    OrderNotFound
from copy import deepcopy
from sccts.exchange_account import ExchangeAccount
from sccts.timeframe import Timeframe
from tests.common import BTC_USD_MARKET, ETH_BTC_MARKET


class ExchangeAccountTest(unittest.TestCase):

    def setUp(self):
        self.dates = pandas.to_datetime(['2017-01-01 1:00', '2017-01-01 1:01',
                                         '2017-01-01 1:02'], utc=True)
        self.timeframe = Timeframe(pd_start_date=self.dates[0],
                                   pd_end_date=self.dates[-1],
                                   pd_timedelta=pandas.Timedelta(minutes=1))
        self.timeframe.add_timedelta()
        data = {'high': [6, 2, 4],
                'low': [5, 0.5, 1]}
        self.eth_btc_ohlcvs = pandas.DataFrame(data=data, index=self.dates)
        data = {'high': [2, 3, 4],
                'low': [1, 2, 3]}
        self.btc_usd_ohlcvs = pandas.DataFrame(data=data, index=self.dates)

    def test__init__ohlcvs_index_start_bigger_than_start_date(self):
        df = self.eth_btc_ohlcvs.drop(self.eth_btc_ohlcvs.index[0])
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def test__init__ohlcvs_index_end_lower_than_end_date(self):
        df = self.eth_btc_ohlcvs.drop(self.eth_btc_ohlcvs.index[-1])
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def test__init__ohlcvs__high_missing(self):
        df = self.eth_btc_ohlcvs.drop('high', 1)
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv high needs to be provided')

    def test__init__ohlcvs__low_missing(self):
        df = self.eth_btc_ohlcvs.drop('low', 1)
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv low needs to be provided')

    def test__init__ohlcvs__wrong_frequency(self):
        df = self.eth_btc_ohlcvs.drop(self.eth_btc_ohlcvs.index[1])
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to be in 1T format')

    def test__init__ohlcvs__not_finite(self):
        df = self.eth_btc_ohlcvs.copy()
        df.iloc[1, 1] = float('inf')
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv ohlcv needs to finite')

    def test__init__ohlcvs__not_convertable_to_float(self):
        df = self.eth_btc_ohlcvs.copy()
        df.iloc[1, 1] = 'asd'
        with self.assertRaises(ValueError) as e:
            ExchangeAccount(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception),
                         "ohlcv could not convert string to float: 'asd'")

    def template__create_order__error(self, exception_text, exception, market,
                                      side, type, amount, price):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3,
                                            'ETH': 0})
        with self.assertRaises(exception) as e:
            account.create_order(market=market, side=side, type=type,
                                 amount=amount, price=price)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 3, 'total': 3, 'used': 0.0},
                          'ETH': {'free': 0, 'total': 0, 'used': 0.0}})
        self.assertEqual(str(e.exception),
                         exception_text)
        self.assertEqual(account.fetch_closed_orders(), [])
        self.assertEqual(account.fetch_open_orders(), [])

    def template__create_order__market_and_limit_error(
            self, exception_text, exception, market, side, amount, price):
        self.template__create_order__error(exception_text=exception_text,
                                           exception=exception, market=market,
                                           side=side, amount=amount,
                                           type='limit',
                                           price=price)
        self.template__create_order__error(exception_text=exception_text,
                                           exception=exception, market=market,
                                           side=side, amount=amount,
                                           type='market',
                                           price=None)

    def test__create_order__unsupported_type(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='stop',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: only market and limit'
                           ' order supported')

    def test__create_order__market__price_set(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='market',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: market order has no price')

    def test__create_order__no_market_provided(self):
        self.template__create_order__market_and_limit_error(
            market=None,
            side='buy',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: market is None')

    def test__create_order__market__has_no_prices(self):
        self.template__create_order__error(
            market=BTC_USD_MARKET,
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: no prices available for BTC/USD')

    def test__create_order__unsupported_side(self):
        self.template__create_order__market_and_limit_error(
            market=BTC_USD_MARKET,
            side='buy',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: no prices available for BTC/USD')

    def test__create_order__market_has_no_quote(self):
        self.template__create_order__market_and_limit_error(
            market={'base': 'ETH', 'symbol': 'ETH/BTC'},
            side='buy',
            amount=5,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: market has no quote')

    def test__create_order__market_has_no_base(self):
        self.template__create_order__market_and_limit_error(
            market={'quote': 'ETH', 'symbol': 'ETH/BTC'},
            side='buy',
            amount=5,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: market has no base')

    def test__create_order__amount_not_finite(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='buy',
            amount=float('inf'),
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be finite')

    def test__create_order__amount_not_a_number(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='buy',
            amount='wrong number',
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be a number')

    def test__create_order__amount_is_zero(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='buy',
            amount=0,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be positive')

    def test__create_order__amount_less_than_zero(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='buy',
            amount=-20,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be positive')

    def test__create_order__limit__price_not_finite(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='limit',
            amount=1,
            price=float('inf'),
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be finite')

    def test__create_order__limit__price_not_a_number(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='limit',
            amount=1,
            price='wrong number',
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be a number')

    def test__create_order__limit__price_is_zero(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='limit',
            amount=1,
            price=0,
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be positive')

    def test__create_order__limit__price_less_than_zero(self):
        self.template__create_order__error(
            market=ETH_BTC_MARKET,
            side='buy',
            type='limit',
            amount=1,
            price=-20,
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be positive')

    def test__create_order__buy__insufficient_funds(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='buy',
            amount=20,
            price=1,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__sell__insufficient_funds(self):
        self.template__create_order__market_and_limit_error(
            market=ETH_BTC_MARKET,
            side='sell',
            amount=20000,
            price=1,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__market_buy__create_balance(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        account.create_order(market=ETH_BTC_MARKET, side='buy',
                             type='market', amount=1, price=None)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 0.997, 'total': 0.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_buy__balance_available(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        self.timeframe.add_timedelta()
        account.create_order(market=ETH_BTC_MARKET, side='buy',
                             type='market', amount=1, price=None)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 2.994, 'total': 2.994, 'used': 0.0},
                          'ETH': {'free': 3.0, 'total': 3.0, 'used': 0.0}})

    def test__create_order__market_sell__create_balance(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        account.create_order(market=ETH_BTC_MARKET, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 0.9985, 'total': 0.9985,
                                  'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_sell__balance_available(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        self.timeframe.add_timedelta()
        account.create_order(market=ETH_BTC_MARKET, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 1.997, 'total': 1.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__limit_buy(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        account.create_order(market=ETH_BTC_MARKET, side='buy',
                             type='limit', amount=2, price=0.5)
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 2, 'used': 1, 'total': 3.0}})

    def test__create_order__limit_sell(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        self.timeframe.add_timedelta()
        account.create_order(market=ETH_BTC_MARKET, side='sell',
                             type='limit', amount=2, price=4)
        self.assertEqual(account.fetch_balance(),
                         {'ETH': {'free': 1.0, 'used': 2.0, 'total': 3.0}})

    def test__fetch_balance(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  balances={'BTC': 15.3,
                                            'USD': 0.3})
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 15.3, 'total': 15.3, 'used': 0.0},
                          'USD': {'free': 0.3, 'total': 0.3, 'used': 0.0}})

    def test__fetch_order__market(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = account.create_order(market=ETH_BTC_MARKET, side='buy',
                                      type='market', amount=1, price=None)
        self.timeframe.add_timedelta()
        sell_id = account.create_order(market=ETH_BTC_MARKET, side='sell',
                                       type='market', amount=1, price=None)
        self.assertEqual(
            account.fetch_order(buy_id['id']),
            {'amount': 1.0,
             'average': 2.003,
             'cost': 2.003,
             'datetime': '2017-01-01T01:01:00.000Z',
             'fee': 0,
             'filled': 1.0,
             'id': '1',
             'info': {},
             'lastTradeTimestamp': 1483232460000,
             'price': 2.003,
             'remaining': 0,
             'side': 'buy',
             'status': 'closed',
             'symbol': 'ETH/BTC',
             'timestamp': 1483232460000,
             'trades': None,
             'type': 'market'})
        self.assertEqual(
            account.fetch_order(sell_id['id']),
            {'amount': 1.0,
             'average': 0.9985,
             'cost': 0.9985,
             'datetime': '2017-01-01T01:02:00.000Z',
             'fee': 0,
             'filled': 1.0,
             'id': '2',
             'info': {},
             'lastTradeTimestamp': 1483232520000,
             'price': 0.9985,
             'remaining': 0,
             'side': 'sell',
             'status': 'closed',
             'symbol': 'ETH/BTC',
             'timestamp': 1483232520000,
             'trades': None,
             'type': 'market'})

    def test__fetch_order__dont_return_internals(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = account.create_order(market=ETH_BTC_MARKET, side='buy',
                                      type='market', amount=1, price=None)
        order = account.fetch_order(buy_id['id'])
        order_copy = deepcopy(order)
        order['info']['test'] = 1
        for key in list(order.keys()):
            del order[key]
        self.assertEqual(order_copy, account.fetch_order(buy_id['id']))

    def test__fetch_order__not_found(self):
        account = ExchangeAccount(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            account.fetch_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: order some_id does not exist')

    def test__fetch_closed_orders__empty(self):
        account = ExchangeAccount(timeframe=self.timeframe)
        self.assertEqual(account.fetch_closed_orders(), [])

    def test__fetch_closed_orders(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                          'BTC/USD': self.btc_usd_ohlcvs},
                                  balances={'BTC': 10})
        market_buy_eth_btc = account.create_order(market=ETH_BTC_MARKET,
                                                  side='buy', type='market',
                                                  amount=2, price=None)['id']
        market_sell_btc_usd = account.create_order(market=BTC_USD_MARKET,
                                                   side='sell', type='market',
                                                   amount=3, price=None)['id']
        account.create_order(market=BTC_USD_MARKET,
                             side='sell', type='limit',
                             amount=1, price=50)
        limit_buy_btc_usd = account.create_order(market=BTC_USD_MARKET,
                                                 side='buy', type='limit',
                                                 amount=0.5, price=3)['id']
        market_buy_eth_btc_order = account.fetch_order(market_buy_eth_btc)
        market_sell_btc_usd_order = account.fetch_order(market_sell_btc_usd)
        self.assertEqual(account.fetch_closed_orders(),
                         [market_buy_eth_btc_order, market_sell_btc_usd_order])
        # This fills limit orders
        self.timeframe.add_timedelta()
        limit_buy_btc_usd_order = account.fetch_order(limit_buy_btc_usd)
        self.assertEqual(account.fetch_closed_orders(),
                         [market_buy_eth_btc_order,
                          market_sell_btc_usd_order,
                          limit_buy_btc_usd_order])
        # Test parameters
        self.assertEqual(account.fetch_closed_orders(symbol='BTC/USD'),
                         [market_sell_btc_usd_order, limit_buy_btc_usd_order])
        self.assertEqual(account.fetch_closed_orders(since=1483232460000),
                         [limit_buy_btc_usd_order])
        self.assertEqual(account.fetch_closed_orders(limit=2),
                         [market_buy_eth_btc_order,
                          market_sell_btc_usd_order])

    def test__fetch_closed_orders__dont_return_internals(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        account.create_order(market=ETH_BTC_MARKET, side='buy',
                             type='market', amount=1, price=None)
        order = account.fetch_closed_orders()[0]
        order_copy = deepcopy(order)
        order['info']['test'] = 1
        for key in list(order.keys()):
            del order[key]
        self.assertEqual([order_copy], account.fetch_closed_orders())

    def test__fetch_open_orders__empty(self):
        account = ExchangeAccount(timeframe=self.timeframe)
        self.assertEqual(account.fetch_open_orders(), [])

    def test__fetch_open_orders(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                          'BTC/USD': self.btc_usd_ohlcvs},
                                  balances={'BTC': 10})
        limit_buy_eth_btc = account.create_order(market=ETH_BTC_MARKET,
                                                 side='buy', type='limit',
                                                 amount=2, price=3)['id']
        limit_buy_eth_btc_order = account.fetch_order(limit_buy_eth_btc)
        limit_sell_btc_usd = account.create_order(market=BTC_USD_MARKET,
                                                  side='sell', type='limit',
                                                  amount=3, price=10)['id']
        limit_sell_btc_usd_order = account.fetch_order(limit_sell_btc_usd)
        account.create_order(market=BTC_USD_MARKET,
                             side='sell', type='market',
                             amount=1, price=None)
        self.assertEqual(account.fetch_open_orders(),
                         [limit_buy_eth_btc_order, limit_sell_btc_usd_order])
        self.timeframe.add_timedelta()
        # order filled
        self.assertEqual(account.fetch_open_orders(),
                         [limit_sell_btc_usd_order])
        limit_buy_btc_usd = account.create_order(market=BTC_USD_MARKET,
                                                 side='buy', type='limit',
                                                 amount=0.5, price=3)['id']
        limit_buy_btc_usd_order = account.fetch_order(limit_buy_btc_usd)
        limit_sell_eth_btc2 = account.create_order(market=ETH_BTC_MARKET,
                                                   side='sell', type='limit',
                                                   amount=0.1, price=2)['id']
        limit_sell_eth_btc2_order = account.fetch_order(limit_sell_eth_btc2)
        self.assertEqual(account.fetch_open_orders(),
                         [limit_sell_btc_usd_order,
                          limit_buy_btc_usd_order,
                          limit_sell_eth_btc2_order])
        # Test parameters
        self.assertEqual(account.fetch_open_orders(symbol='BTC/USD'),
                         [limit_sell_btc_usd_order, limit_buy_btc_usd_order])
        self.assertEqual(account.fetch_open_orders(since=1483232460001),
                         [limit_buy_btc_usd_order, limit_sell_eth_btc2_order])
        self.assertEqual(account.fetch_open_orders(limit=2),
                         [limit_sell_btc_usd_order, limit_buy_btc_usd_order])
        self.assertEqual(account.fetch_open_orders(since=1483232460001, limit=1),
                         [limit_buy_btc_usd_order])

    def test__fetch_open_orders__dont_return_internals(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        account.create_order(market=ETH_BTC_MARKET, side='buy',
                             type='limit', amount=1, price=2)
        order = account.fetch_open_orders()[0]
        order_copy = deepcopy(order)
        order['info']['test'] = 1
        for key in list(order.keys()):
            del order[key]
        self.assertEqual([order_copy], account.fetch_open_orders())

    def test__cancel_order__market(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = account.create_order(market=ETH_BTC_MARKET, side='buy',
                                      type='market', amount=1, price=None)
        with self.assertRaises(BadRequest) as e:
            account.cancel_order(buy_id['id'])
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')

    def test__cancel_order__not_found(self):
        account = ExchangeAccount(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            account.cancel_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: order some_id does not exist')

    def test__cancel_order__limit_buy(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        create_result = account.create_order(market=ETH_BTC_MARKET,
                                             side='buy', type='limit',
                                             amount=2, price=1)
        result = account.cancel_order(id=create_result['id'])
        self.assertEqual(result, {'id': create_result['id'],
                                  'info': {}})

        def check_canceled():
            self.assertEqual(account.fetch_balance(),
                             {'BTC': {'free': 3.0, 'total': 3.0, 'used': 0.0}})
            self.assertEqual(account.fetch_order(id=result['id']),
                             {'amount': 2.0,
                              'average': None,
                              'cost': None,
                              'datetime': '2017-01-01T01:01:00.000Z',
                              'fee': 0,
                              'filled': 0,
                              'id': '1',
                              'info': {},
                              'lastTradeTimestamp': None,
                              'price': None,
                              'remaining': 2.0,
                              'side': 'buy',
                              'status': 'canceled',
                              'symbol': 'ETH/BTC',
                              'timestamp': 1483232460000,
                              'trades': None,
                              'type': 'limit'})

        check_canceled()
        # Order gets filled next run, check if it is handling it correct
        self.timeframe.add_timedelta()
        check_canceled()

    def test__cancel_order__limit_sell(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        create_result = account.create_order(market=ETH_BTC_MARKET,
                                             side='sell', type='limit',
                                             amount=2, price=10)
        result = account.cancel_order(id=create_result['id'])
        self.assertEqual(result, {'id': create_result['id'],
                                  'info': {}})
        self.assertEqual(account.fetch_balance(),
                         {'ETH': {'free': 3.0, 'total': 3.0, 'used': 0.0}})
        self.assertEqual(account.fetch_order(id=result['id']),
                         {'amount': 2.0,
                          'average': None,
                          'cost': None,
                          'datetime': '2017-01-01T01:01:00.000Z',
                          'fee': 0,
                          'filled': 0,
                          'id': '1',
                          'info': {},
                          'lastTradeTimestamp': None,
                          'price': None,
                          'remaining': 2.0,
                          'side': 'sell',
                          'status': 'canceled',
                          'symbol': 'ETH/BTC',
                          'timestamp': 1483232460000,
                          'trades': None,
                          'type': 'limit'})

    def test__cancel_order__already_canceled(self):
        account = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        create_result = account.create_order(market=ETH_BTC_MARKET,
                                             side='sell', type='limit',
                                             amount=2, price=10)
        account.cancel_order(id=create_result['id'])
        with self.assertRaises(BadRequest) as e:
            account.cancel_order(id=create_result['id'])
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel canceled order 1')
        self.assertEqual(account.fetch_balance(),
                         {'ETH': {'free': 3.0, 'total': 3.0, 'used': 0.0}})

    def setup_update_state_limit_sell(self):
        timeframe = Timeframe(pd_start_date=self.dates[0],
                              pd_end_date=self.dates[-1],
                              pd_timedelta=pandas.Timedelta(minutes=1))
        data = {'high': [3, 4, 5],
                'low': [2, 3, 4]}
        eth_btc_ohlcvs = pandas.DataFrame(data=data, index=self.dates)
        data = {'high': [1, 1, 1],
                'low': [1, 1, 1]}
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=self.dates)
        account = ExchangeAccount(timeframe=timeframe,
                                  ohlcvs={'ETH/BTC': eth_btc_ohlcvs,
                                          'BTC/USD': btc_usd_ohlcvs},
                                  balances={'ETH': 3})
        create_result = account.create_order(market=ETH_BTC_MARKET,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        order_id = create_result['id']
        self.check_update_state_limit_sell_not_filled(account, order_id)
        timeframe.add_timedelta()
        self.check_update_state_limit_sell_not_filled(account, order_id)
        timeframe.add_timedelta()
        return account, timeframe, order_id

    def update_state_limit_sell_order_not_filled(self):
        return {'amount': 2.0,
                'average': None,
                'cost': None,
                'datetime': '2017-01-01T01:00:00.000Z',
                'fee': 0,
                'filled': 0,
                'id': '1',
                'info': {},
                'lastTradeTimestamp': None,
                'price': None,
                'remaining': 2.0,
                'side': 'sell',
                'status': 'open',
                'symbol': 'ETH/BTC',
                'timestamp': 1483232400000,
                'trades': None,
                'type': 'limit'}

    def check_update_state_limit_sell_fetch_order_not_filled(
            self, account, id_):
        self.assertEqual(account.fetch_order(id_),
                         self.update_state_limit_sell_order_not_filled())

    def check_update_state_limit_sell_fetch_open_orders_not_filled(
            self, account):
        self.assertEqual(account.fetch_open_orders(),
                         [self.update_state_limit_sell_order_not_filled()])

    def check_update_state_limit_sell_fetch_closed_orders_not_filled(
            self, account):
        self.assertEqual(account.fetch_closed_orders(), [])

    def check_update_state_limit_sell_fetch_balance_not_filled(self, account):
        self.assertEqual(account.fetch_balance(),
                         {'ETH': {'free': 1.0, 'total': 3.0, 'used': 2.0}})

    def check_update_state_limit_sell_not_filled(self, account, order_id):
        self.check_update_state_limit_sell_fetch_order_not_filled(
            account, order_id)
        self.check_update_state_limit_sell_fetch_balance_not_filled(account)
        self.check_update_state_limit_sell_fetch_closed_orders_not_filled(
            account)
        self.check_update_state_limit_sell_fetch_open_orders_not_filled(
            account)

    def update_state_limit_sell_order_filled(self):
        return {'amount': 2.0,
                'average': 5.0,
                'cost': 10,
                'datetime': '2017-01-01T01:00:00.000Z',
                'fee': 0,
                'filled': 2.0,
                'id': '1',
                'info': {},
                'lastTradeTimestamp': 1483232520000,
                'price': 5.0,
                'remaining': 0.0,
                'side': 'sell',
                'status': 'closed',
                'symbol': 'ETH/BTC',
                'timestamp': 1483232400000,
                'trades': None,
                'type': 'limit'}

    def check_update_state_limit_sell_fetch_order_filled(self, account, id_):
        self.assertEqual(account.fetch_order(id_),
                         self.update_state_limit_sell_order_filled())

    def check_update_state_limit_sell_fetch_open_orders_filled(self, account):
        self.assertEqual(account.fetch_open_orders(), [])

    def check_update_state_limit_sell_fetch_closed_orders_filled(
            self, account):
        self.assertEqual(account.fetch_closed_orders(limit=5),
                         [self.update_state_limit_sell_order_filled()])

    def check_update_state_limit_sell_fetch_balance_filled(self, account):
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 10.0, 'total': 10.0, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def check_update_state_limit_sell_filled(self, account, order_id):
        self.check_update_state_limit_sell_fetch_order_filled(
            account, order_id)
        self.check_update_state_limit_sell_fetch_balance_filled(account)
        self.check_update_state_limit_sell_fetch_closed_orders_filled(account)
        self.check_update_state_limit_sell_fetch_open_orders_filled(account)

    def test__update_state__create_order__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        # Check if balance is available when first calling create_order
        create_result = account.create_order(market=BTC_USD_MARKET,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        # cancel, so there is no used balance
        account.cancel_order(create_result['id'])
        self.check_update_state_limit_sell_filled(account, order_id)

    def test__update_state__fetch_balance__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_fetch_balance_filled(account)
        self.check_update_state_limit_sell_filled(account, order_id)

    def test__update_state__fetch_order__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_fetch_order_filled(
            account, order_id)
        self.check_update_state_limit_sell_filled(account, order_id)

    def test__update_state__fetch_closed_orders__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_fetch_closed_orders_filled(account)
        self.check_update_state_limit_sell_filled(account, order_id)

    def test__update_state__fetch_open_orders__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_fetch_open_orders_filled(account)
        self.check_update_state_limit_sell_filled(account, order_id)

    def test__update_state__cancel_order__limit_sell(self):
        account, timeframe, order_id = self.setup_update_state_limit_sell()
        with self.assertRaises(BadRequest) as e:
            account.cancel_order(id=order_id)
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')
        self.check_update_state_limit_sell_filled(account, order_id)

    def setup_update_state_limit_buy(self):
        timeframe = Timeframe(pd_start_date=self.dates[0],
                              pd_end_date=self.dates[-1],
                              pd_timedelta=pandas.Timedelta(minutes=1))
        data = {'high': [7, 6, 5],
                'low': [6, 5, 4]}
        eth_btc_ohlcvs = pandas.DataFrame(data=data, index=self.dates)
        data = {'high': [1, 1, 1],
                'low': [1, 1, 1]}
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=self.dates)
        account = ExchangeAccount(timeframe=timeframe,
                                  ohlcvs={'ETH/BTC': eth_btc_ohlcvs,
                                          'BTC/USD': btc_usd_ohlcvs},
                                  balances={'BTC': 15})
        create_result = account.create_order(market=ETH_BTC_MARKET,
                                             side='buy', type='limit',
                                             amount=1.5, price=4)
        order_id = create_result['id']
        self.check_update_state_limit_buy_not_filled(account, order_id)
        timeframe.add_timedelta()
        self.check_update_state_limit_buy_not_filled(account, order_id)
        timeframe.add_timedelta()
        return account, timeframe, order_id

    def update_state_limit_buy_order_not_filled(self):
        return {'amount': 1.5,
                'average': None,
                'cost': None,
                'datetime': '2017-01-01T01:00:00.000Z',
                'fee': 0,
                'filled': 0,
                'id': '1',
                'info': {},
                'lastTradeTimestamp': None,
                'price': None,
                'remaining': 1.5,
                'side': 'buy',
                'status': 'open',
                'symbol': 'ETH/BTC',
                'timestamp': 1483232400000,
                'trades': None,
                'type': 'limit'}

    def check_update_state_limit_buy_fetch_order_not_filled(
            self, account, id_):
        self.assertEqual(account.fetch_order(id_),
                         self.update_state_limit_buy_order_not_filled())

    def check_update_state_limit_buy_fetch_closed_orders_not_filled(
            self, account):
        self.assertEqual(account.fetch_closed_orders(),
                         [])

    def check_update_state_limit_buy_fetch_open_orders_not_filled(
            self, account):
        self.assertEqual(account.fetch_open_orders(),
                         [self.update_state_limit_buy_order_not_filled()])

    def check_update_state_limit_buy_fetch_balance_not_filled(self, account):
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 9.0, 'total': 15.0, 'used': 6.0}})

    def check_update_state_limit_buy_not_filled(self, account, order_id):
        self.check_update_state_limit_buy_fetch_order_not_filled(
            account, order_id)
        self.check_update_state_limit_buy_fetch_balance_not_filled(account)
        self.check_update_state_limit_buy_fetch_closed_orders_not_filled(
            account)
        self.check_update_state_limit_buy_fetch_open_orders_not_filled(account)

    def update_state_limit_buy_order_filled(self):
        return {'amount': 1.5,
                'average': 4.0,
                'cost': 6,
                'datetime': '2017-01-01T01:00:00.000Z',
                'fee': 0,
                'filled': 1.5,
                'id': '1',
                'info': {},
                'lastTradeTimestamp': 1483232520000,
                'price': 4.0,
                'remaining': 0.0,
                'side': 'buy',
                'status': 'closed',
                'symbol': 'ETH/BTC',
                'timestamp': 1483232400000,
                'trades': None,
                'type': 'limit'}

    def check_update_state_limit_buy_fetch_order_filled(self, account, id_):
        self.assertEqual(account.fetch_order(id_),
                         self.update_state_limit_buy_order_filled())

    def check_update_state_limit_buy_fetch_closed_orders_filled(self, account):
        self.assertEqual(account.fetch_closed_orders(limit=5),
                         [self.update_state_limit_buy_order_filled()])

    def check_update_state_limit_buy_fetch_open_orders_filled(self, account):
        self.assertEqual(account.fetch_open_orders(limit=5), [])

    def check_update_state_limit_buy_fetch_balance_filled(self, account):
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 9.0, 'total': 9.0, 'used': 0.0},
                          'ETH': {'free': 1.5, 'total': 1.5, 'used': 0.0}})

    def check_update_state_limit_buy_filled(self, account, order_id):
        self.check_update_state_limit_buy_fetch_order_filled(account, order_id)
        self.check_update_state_limit_buy_fetch_balance_filled(account)
        self.check_update_state_limit_buy_fetch_closed_orders_filled(account)
        self.check_update_state_limit_buy_fetch_open_orders_filled(account)

    def test__update_state__create_order__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        # Check if balance is available when first calling create_order
        create_result = account.create_order(market=BTC_USD_MARKET,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        # cancel, so there is no used balance
        account.cancel_order(create_result['id'])
        self.check_update_state_limit_buy_filled(account, order_id)

    def test__update_state__fetch_balance__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_fetch_balance_filled(account)
        self.check_update_state_limit_buy_filled(account, order_id)

    def test__update_state__fetch_order__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_fetch_order_filled(account, order_id)
        self.check_update_state_limit_buy_filled(account, order_id)

    def test__update_state__fetch_closed_orders__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_fetch_closed_orders_filled(account)
        self.check_update_state_limit_buy_filled(account, order_id)

    def test__update_state__fetch_open_orders__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_fetch_open_orders_filled(account)
        self.check_update_state_limit_buy_filled(account, order_id)

    def test__update_state__cancel_order__limit_buy(self):
        account, timeframe, order_id = self.setup_update_state_limit_buy()
        with self.assertRaises(BadRequest) as e:
            account.cancel_order(id=order_id)
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')
        self.check_update_state_limit_buy_filled(account, order_id)
