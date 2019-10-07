import pandas
import unittest
from ccxt.base.errors import BadRequest, InsufficientFunds, InvalidOrder, \
    OrderNotFound, BadSymbol
from decimal import Decimal
from unittest.mock import patch, MagicMock
from sccts.exchange_backend import Balance, ExchangeAccount, ExchangeBackend
from sccts.backtest import Timeframe


class BalanceTest(unittest.TestCase):

    def test__default_init(self):
        balance = Balance()
        self.assertEqual(balance.to_dict(), {
            'free': Decimal(0),
            'used': Decimal(0),
            'total': Decimal(0),
        })

    def test__positive_initialization(self):
        balance = Balance(15.3)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__negative_initialization(self):
        with self.assertRaises(ValueError) as e:
            Balance(-1)
        self.assertEqual(str(e.exception),
                         'Balance: inital value cant be negative')

    def test__change_total__add(self):
        balance = Balance(15.3)
        balance.change_total(0.7)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('16.0'),
            'used': Decimal(0),
            'total': Decimal('16.0'),
        })

    def test__change_total__substraction(self):
        balance = Balance(15.3)
        balance.change_total(-0.3)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.0'),
            'used': Decimal(0),
            'total': Decimal('15.0'),
        })

    def test__change_total__to_negative(self):
        balance = Balance(15.3)
        with self.assertRaises(InsufficientFunds) as e:
            balance.change_total(-16)
        self.assertEqual(str(e.exception), 'Balance too little')
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__change_used__add(self):
        balance = Balance(15.3)
        balance.change_used(0.5)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('14.8'),
            'used': Decimal(0.5),
            'total': Decimal('15.3'),
        })

    def test__change_used__substraction(self):
        balance = Balance(15.3)
        balance.change_used(-0.5)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.8'),
            'used': Decimal(-0.5),
            'total': Decimal('15.3'),
        })

    def test__change_used__to_negative(self):
        balance = Balance(15.3)
        with self.assertRaises(InsufficientFunds) as e:
            balance.change_used(16)
        self.assertEqual(str(e.exception), 'Balance too little')
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__repr(self):
        balance = Balance(15.3)
        balance.change_used(0.5)
        self.assertEqual(str(balance),
                         "{'free': Decimal('14.8'), 'used': Decimal('0.5'), "
                         "'total': Decimal('15.3')}")


class ExchangeAccountTest(unittest.TestCase):

    def setUp(self):
        self.eth_btc_market = {'base': 'ETH', 'quote': 'BTC',
                               'symbol': 'ETH/BTC'}
        self.btc_usd_market = {'base': 'BTC', 'quote': 'USD',
                               'symbol': 'BTC/USD'}
        self.dates = pandas.to_datetime(['2017-01-01 1:00', '2017-01-01 1:01',
                                         '2017-01-01 1:02'], utc=True)
        self.timeframe = Timeframe(pd_start_date=self.dates[0],
                                   pd_end_date=self.dates[-1],
                                   pd_timedelta=pandas.Timedelta(minutes=1))
        self.timeframe.add_timedelta()
        data = {'high': [6, 2, 4],
                'low': [5, 0.5, 1]}
        self.eth_btc_ohlcvs = pandas.DataFrame(data=data, index=self.dates)

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
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3,
                                            'ETH': 0})
        with self.assertRaises(exception) as e:
            backend.create_order(market=market, side=side, type=type,
                                 amount=amount, price=price)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 3, 'total': 3, 'used': 0.0},
                          'ETH': {'free': 0, 'total': 0, 'used': 0.0}})
        self.assertEqual(str(e.exception),
                         exception_text)
        # TODO: Test fetch_open_orders / fetch_closed_orders empty

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
            market=self.eth_btc_market,
            side='buy',
            type='stop',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: only market and limit'
                           ' order supported')

    def test__create_order__market__price_set(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
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
            market=self.btc_usd_market,
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=InvalidOrder,
            exception_text='ExchangeAccount: no prices available for BTC/USD')

    def test__create_order__unsupported_side(self):
        self.template__create_order__market_and_limit_error(
            market=self.btc_usd_market,
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
            market=self.eth_btc_market,
            side='buy',
            amount=float('inf'),
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be finite')

    def test__create_order__amount_not_a_number(self):
        self.template__create_order__market_and_limit_error(
            market=self.eth_btc_market,
            side='buy',
            amount='wrong number',
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be a number')

    def test__create_order__amount_is_zero(self):
        self.template__create_order__market_and_limit_error(
            market=self.eth_btc_market,
            side='buy',
            amount=0,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be positive')

    def test__create_order__amount_less_than_zero(self):
        self.template__create_order__market_and_limit_error(
            market=self.eth_btc_market,
            side='buy',
            amount=-20,
            price=1,
            exception=BadRequest,
            exception_text='ExchangeAccount: amount needs to be positive')

    def test__create_order__limit__price_not_finite(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='limit',
            amount=1,
            price=float('inf'),
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be finite')

    def test__create_order__limit__price_not_a_number(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='limit',
            amount=1,
            price='wrong number',
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be a number')

    def test__create_order__limit__price_is_zero(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='limit',
            amount=1,
            price=0,
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be positive')

    def test__create_order__limit__price_less_than_zero(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='limit',
            amount=1,
            price=-20,
            exception=BadRequest,
            exception_text='ExchangeAccount: price needs to be positive')

    def test__create_order__buy__insufficient_funds(self):
        self.template__create_order__market_and_limit_error(
            market=self.eth_btc_market,
            side='buy',
            amount=20,
            price=1,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__sell__insufficient_funds(self):
        self.template__create_order__market_and_limit_error(
            market=self.eth_btc_market,
            side='sell',
            amount=20000,
            price=1,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__market_buy__create_balance(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        backend.create_order(market=self.eth_btc_market, side='buy',
                             type='market', amount=1, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 0.997, 'total': 0.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_buy__balance_available(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        self.timeframe.add_timedelta()
        backend.create_order(market=self.eth_btc_market, side='buy',
                             type='market', amount=1, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 2.994, 'total': 2.994, 'used': 0.0},
                          'ETH': {'free': 3.0, 'total': 3.0, 'used': 0.0}})

    def test__create_order__market_sell__create_balance(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        backend.create_order(market=self.eth_btc_market, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 0.9985, 'total': 0.9985,
                                  'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_sell__balance_available(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        self.timeframe.add_timedelta()
        backend.create_order(market=self.eth_btc_market, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 1.997, 'total': 1.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__limit_buy(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        backend.create_order(market=self.eth_btc_market, side='buy',
                             type='limit', amount=2, price=0.5)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 2, 'used': 1, 'total': 3.0}})

    def test__create_order__limit_sell(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        self.timeframe.add_timedelta()
        backend.create_order(market=self.eth_btc_market, side='sell',
                             type='limit', amount=2, price=4)
        self.assertEqual(backend.fetch_balance(),
                         {'ETH': {'free': 1.0, 'used': 2.0, 'total': 3.0}})

    def test__fetch_balance(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  balances={'BTC': 15.3,
                                            'USD': 0.3})
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 15.3, 'total': 15.3, 'used': 0.0},
                          'USD': {'free': 0.3, 'total': 0.3, 'used': 0.0}})

    def test__fetch_order__market(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = backend.create_order(market=self.eth_btc_market, side='buy',
                                      type='market', amount=1, price=None)
        self.timeframe.add_timedelta()
        sell_id = backend.create_order(market=self.eth_btc_market, side='sell',
                                       type='market', amount=1, price=None)
        self.assertEqual(
            backend.fetch_order(buy_id['id']),
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
            backend.fetch_order(sell_id['id']),
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
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = backend.create_order(market=self.eth_btc_market, side='buy',
                                      type='market', amount=1, price=None)
        order = backend.fetch_order(buy_id['id'])
        order_copy = order.copy()
        for key in list(order.keys()):
            del order[key]
        self.assertEqual(order_copy, backend.fetch_order(buy_id['id']))

    def test__fetch_order__not_found(self):
        backend = ExchangeAccount(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            backend.fetch_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: order some_id does not exist')

    def test__cancel_order__market(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = backend.create_order(market=self.eth_btc_market, side='buy',
                                      type='market', amount=1, price=None)
        with self.assertRaises(BadRequest) as e:
            backend.cancel_order(buy_id['id'])
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')

    def test__cancel_order__not_found(self):
        backend = ExchangeAccount(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            backend.cancel_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: order some_id does not exist')

    def test__cancel_order__limit_buy(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        create_result = backend.create_order(market=self.eth_btc_market,
                                             side='buy', type='limit',
                                             amount=2, price=1)
        result = backend.cancel_order(id=create_result['id'])
        self.assertEqual(result, {'id': create_result['id'],
                                  'info': {}})
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 3.0, 'total': 3.0, 'used': 0.0}})
        self.assertEqual(backend.fetch_order(id=result['id']),
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

    def test__cancel_order__limit_sell(self):
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        create_result = backend.create_order(market=self.eth_btc_market,
                                             side='sell', type='limit',
                                             amount=2, price=10)
        result = backend.cancel_order(id=create_result['id'])
        self.assertEqual(result, {'id': create_result['id'],
                                  'info': {}})
        self.assertEqual(backend.fetch_balance(),
                         {'ETH': {'free': 3.0, 'total': 3.0, 'used': 0.0}})
        self.assertEqual(backend.fetch_order(id=result['id']),
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
        backend = ExchangeAccount(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        create_result = backend.create_order(market=self.eth_btc_market,
                                             side='sell', type='limit',
                                             amount=2, price=10)
        backend.cancel_order(id=create_result['id'])
        with self.assertRaises(BadRequest) as e:
            backend.cancel_order(id=create_result['id'])
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel canceled order 1')
        self.assertEqual(backend.fetch_balance(),
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
        backend = ExchangeAccount(timeframe=timeframe,
                                  ohlcvs={'ETH/BTC': eth_btc_ohlcvs,
                                          'BTC/USD': btc_usd_ohlcvs},
                                  balances={'ETH': 3})
        create_result = backend.create_order(market=self.eth_btc_market,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        order_id = create_result['id']
        self.check_update_state_limit_sell_not_filled(backend, order_id)
        timeframe.add_timedelta()
        self.check_update_state_limit_sell_not_filled(backend, order_id)
        timeframe.add_timedelta()
        return backend, timeframe, order_id

    def check_update_state_limit_sell_order_not_filled(self, backend, id_):
        self.assertEqual(backend.fetch_order(id_),
                         {'amount': 2.0,
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
                          'type': 'limit'})

    def check_update_state_limit_sell_fetch_balance_not_filled(self, backend):
        self.assertEqual(backend.fetch_balance(),
                         {'ETH': {'free': 1.0, 'total': 3.0, 'used': 2.0}})

    def check_update_state_limit_sell_not_filled(self, backend, order_id):
        self.check_update_state_limit_sell_order_not_filled(backend, order_id)
        self.check_update_state_limit_sell_fetch_balance_not_filled(backend)
        # TODO: open orders, closed orders

    def check_update_state_limit_sell_order_filled(self, backend, id_):
        self.assertEqual(backend.fetch_order(id_),
                         {'amount': 2.0,
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
                          'type': 'limit'})

    def check_update_state_limit_sell_fetch_balance_filled(self, backend):
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 10.0, 'total': 10.0, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def check_update_state_limit_sell_filled(self, backend, order_id):
        self.check_update_state_limit_sell_order_filled(backend, order_id)
        self.check_update_state_limit_sell_fetch_balance_filled(backend)
        # TODO: open orders, closed orders

    def test__update_state__create_order__limit_sell(self):
        backend, timeframe, order_id = self.setup_update_state_limit_sell()
        # Check if balance is available when first calling create_order
        create_result = backend.create_order(market=self.btc_usd_market,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        # cancel, so there is no used balance
        backend.cancel_order(create_result['id'])
        self.check_update_state_limit_sell_filled(backend, order_id)

    def test__update_state__fetch_balance__limit_sell(self):
        backend, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_fetch_balance_filled(backend)
        self.check_update_state_limit_sell_filled(backend, order_id)

    def test__update_state__fetch_order__limit_sell(self):
        backend, timeframe, order_id = self.setup_update_state_limit_sell()
        # first check if this method return correct
        self.check_update_state_limit_sell_order_filled(backend, order_id)
        self.check_update_state_limit_sell_filled(backend, order_id)

    def test__update_state__cancel_order__limit_sell(self):
        backend, timeframe, order_id = self.setup_update_state_limit_sell()
        with self.assertRaises(BadRequest) as e:
            backend.cancel_order(id=order_id)
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')
        self.check_update_state_limit_sell_filled(backend, order_id)

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
        backend = ExchangeAccount(timeframe=timeframe,
                                  ohlcvs={'ETH/BTC': eth_btc_ohlcvs,
                                          'BTC/USD': btc_usd_ohlcvs},
                                  balances={'BTC': 15})
        create_result = backend.create_order(market=self.eth_btc_market,
                                             side='buy', type='limit',
                                             amount=1.5, price=4)
        order_id = create_result['id']
        self.check_update_state_limit_buy_not_filled(backend, order_id)
        timeframe.add_timedelta()
        self.check_update_state_limit_buy_not_filled(backend, order_id)
        timeframe.add_timedelta()
        return backend, timeframe, order_id

    def check_update_state_limit_buy_order_not_filled(self, backend, id_):
        self.assertEqual(backend.fetch_order(id_),
                         {'amount': 1.5,
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
                          'type': 'limit'})

    def check_update_state_limit_buy_fetch_balance_not_filled(self, backend):
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 9.0, 'total': 15.0, 'used': 6.0}})

    def check_update_state_limit_buy_not_filled(self, backend, order_id):
        self.check_update_state_limit_buy_order_not_filled(backend, order_id)
        self.check_update_state_limit_buy_fetch_balance_not_filled(backend)
        # TODO: open orders, closed orders

    def check_update_state_limit_buy_order_filled(self, backend, id_):
        self.assertEqual(backend.fetch_order(id_),
                         {'amount': 1.5,
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
                          'type': 'limit'})

    def check_update_state_limit_buy_fetch_balance_filled(self, backend):
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 9.0, 'total': 9.0, 'used': 0.0},
                          'ETH': {'free': 1.5, 'total': 1.5, 'used': 0.0}})

    def check_update_state_limit_buy_filled(self, backend, order_id):
        self.check_update_state_limit_buy_order_filled(backend, order_id)
        self.check_update_state_limit_buy_fetch_balance_filled(backend)
        # TODO: open orders, closed orders

    def test__update_state__create_order__limit_buy(self):
        backend, timeframe, order_id = self.setup_update_state_limit_buy()
        # Check if balance is available when first calling create_order
        create_result = backend.create_order(market=self.btc_usd_market,
                                             side='sell', type='limit',
                                             amount=2, price=5)
        # cancel, so there is no used balance
        backend.cancel_order(create_result['id'])
        self.check_update_state_limit_buy_filled(backend, order_id)

    def test__update_state__fetch_balance__limit_buy(self):
        backend, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_fetch_balance_filled(backend)
        self.check_update_state_limit_buy_filled(backend, order_id)

    def test__update_state__fetch_order__limit_buy(self):
        backend, timeframe, order_id = self.setup_update_state_limit_buy()
        # first check if this method return correct
        self.check_update_state_limit_buy_order_filled(backend, order_id)
        self.check_update_state_limit_buy_filled(backend, order_id)

    def test__update_state__cancel_order__limit_buy(self):
        backend, timeframe, order_id = self.setup_update_state_limit_buy()
        with self.assertRaises(BadRequest) as e:
            backend.cancel_order(id=order_id)
        self.assertEqual(str(e.exception),
                         'ExchangeAccount: cannot cancel closed order 1')
        self.check_update_state_limit_buy_filled(backend, order_id)


class ExchangeBackendTest(unittest.TestCase):

    def setUp(self):
        self.btc_usd_market = {'base': 'BTC', 'quote': 'USD',
                               'symbol': 'BTC/USD'}
        dates = pandas.to_datetime(['2017-01-01 1:00', '2017-01-01 1:01',
                                    '2017-01-01 1:02'], utc=True)
        self.init_timeframe = Timeframe(pd_start_date=dates[0],
                                        pd_end_date=dates[-1],
                                        pd_timedelta=pandas.Timedelta(
                                            minutes=1))
        data = {'open': [4, 7, 11],
                'high': [5, 8, 12],
                'low': [3, 6, 10],
                'close': [7, 11, 15],
                'volume': [101, 105, 110]}
        self.init_ohlcvs = pandas.DataFrame(data=data, index=dates)
        dates = pandas.date_range(
            start='2017-01-01 1:01', end='2017-01-01 1:20',
            freq='1T', tz='UTC')
        data = {'open': [4 + 4 * i for i in range(0, 20)],
                'high': [5 + 4 * i for i in range(0, 20)],
                'low': [3 + 4 * i for i in range(0, 20)],
                'close': [8 + 4 * i for i in range(0, 20)],
                'volume': [100 + 4 * i for i in range(0, 20)]}
        self.fetch_ohlcv_ohlcvs = pandas.DataFrame(data=data, index=dates)
        self.fetch_ohlcv_timeframe = Timeframe(
            pd_start_date=dates[17], pd_end_date=dates[-1],
            pd_timedelta=pandas.Timedelta(minutes=1))
        self.fetch_ohlcv_timeframe.add_timedelta()

    def test__init__ohlcvs__index_start_bigger_than_start_date(self):
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': self.init_ohlcvs[1:]},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def test__init__ohlcvs__index_end_lower_than_end_date(self):
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': self.init_ohlcvs[:2]},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def template__init__ohlcvs__missing(self, column):
        df = self.init_ohlcvs.drop(column, 1)
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception),
                         'ohlcv {} needs to be provided'.format(column))

    def test__init__ohlcvs__open_missing(self):
        self.template__init__ohlcvs__missing('open')

    def test__init__ohlcvs__high_missing(self):
        self.template__init__ohlcvs__missing('high')

    def test__init__ohlcvs__low_missing(self):
        self.template__init__ohlcvs__missing('low')

    def test__init__ohlcvs__close_missing(self):
        self.template__init__ohlcvs__missing('close')

    def test__init__ohlcvs__volume_missing(self):
        self.template__init__ohlcvs__missing('volume')

    def test__init__ohlcvs__wrong_frequency(self):
        df = self.init_ohlcvs.drop(self.init_ohlcvs.index[1])
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to be in 1T format')

    def test__init__ohlcvs__not_finite(self):
        df = self.init_ohlcvs.copy()
        df.iloc[1, 1] = float('inf')
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv ohlcv needs to finite')

    def test__init__ohlcvs__not_convertable_to_float(self):
        df = self.init_ohlcvs.copy()
        df.iloc[1, 1] = 'asd'
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.init_timeframe,
                            ohlcvs={'ETH/BTC': df},
                            balances={})
        self.assertEqual(str(e.exception),
                         "ohlcv could not convert string to float: 'asd'")

    @patch("sccts.exchange_backend.ExchangeAccount")
    def test__init(self, mock):
        ohlcvs_mock = MagicMock()
        timeframe_mock = MagicMock()
        balances_mock = MagicMock()
        ExchangeBackend(ohlcvs=ohlcvs_mock,
                        timeframe=timeframe_mock,
                        balances=balances_mock)
        mock.assert_called_once_with(ohlcvs=ohlcvs_mock,
                                     timeframe=timeframe_mock,
                                     balances=balances_mock)

    @patch("sccts.exchange_backend.ExchangeAccount")
    def template_exchange_account_method_propagated(
            self, mock, kwargs, methodname):
        ohlcvs = {}
        timeframe_mock = MagicMock()
        balances = {}
        backend = ExchangeBackend(ohlcvs=ohlcvs,
                                  timeframe=timeframe_mock,
                                  balances=balances)
        result = getattr(backend, methodname)(**kwargs)
        mock.assert_called_once_with(ohlcvs=ohlcvs,
                                     timeframe=timeframe_mock,
                                     balances=balances)
        getattr(mock(), methodname).assert_called_once_with(**kwargs)
        self.assertEqual(result, getattr(mock(), methodname)())

    def test__create_order(self):
        self.template_exchange_account_method_propagated(
            kwargs={'market': {}, 'side': 'sell', 'price': 5,
                    'amount': 10, 'type': 'limit'},
            methodname='create_order')

    def test__cancel_order(self):
        self.template_exchange_account_method_propagated(
            kwargs={'id': '123', 'symbol': None},
            methodname='cancel_order')

    def test__fetch_order(self):
        self.template_exchange_account_method_propagated(
            kwargs={'id': '123', 'symbol': None},
            methodname='fetch_order')

    def test__fetch_balance(self):
        self.template_exchange_account_method_propagated(
            kwargs={},
            methodname='fetch_balance')

    def test__fetch_ohlcv_dataframe__no_data(self):
        backend = ExchangeBackend(ohlcvs={},
                                  timeframe=MagicMock(),
                                  balances={})
        with self.assertRaises(BadSymbol) as e:
            backend.fetch_ohlcv_dataframe('UNK/BTC', '1m')
        self.assertEqual(str(e.exception),
                         'ExchangeBackend: no prices for UNK/BTC')

    def test__fetch_ohlcv_dataframe__access_future(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        with self.assertRaises(BadRequest) as e:
            backend.fetch_ohlcv_dataframe(
                symbol=symbol, timeframe='1m', since=1483232610000, limit=17)
        self.assertEqual(
            str(e.exception),
            'ExchangeBackend: fetch_ohlcv: since.ceil(timeframe) + limit'
            ' * timeframe needs to be in the past')

    def test__fetch_ohlcv_dataframe__access_future_timeframe(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        with self.assertRaises(BadRequest) as e:
            backend.fetch_ohlcv_dataframe(
                symbol=symbol, timeframe='2m', since=1483232610000, limit=9)
        self.assertEqual(
            str(e.exception),
            'ExchangeBackend: fetch_ohlcv: since.ceil(timeframe) + limit'
            ' * timeframe needs to be in the past')

    def test__fetch_ohlcv_dataframe(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [4, 8, 12, 16, 20],
                    'high': [5, 9, 13, 17, 21],
                    'low': [3, 7, 11, 15, 19],
                    'close': [8, 12, 16, 20, 24],
                    'volume': [100, 104, 108, 112, 116]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:01', '2017-01-01 1:05', 5, tz='UTC')))

    def test__fetch_ohlcv_dataframe__limit(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol, limit=3)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [4, 8, 12],
                    'high': [5, 9, 13],
                    'low': [3, 7, 11],
                    'close': [8, 12, 16],
                    'volume': [100, 104, 108]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:01', '2017-01-01 1:03', 3, tz='UTC')))

    def test__fetch_ohlcv_dataframe__since(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol,
                                               since=1483232790000)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [28, 32, 36, 40, 44],
                    'high': [29, 33, 37, 41, 45],
                    'low': [27, 31, 35, 39, 43],
                    'close': [32, 36, 40, 44, 48],
                    'volume': [124, 128, 132, 136, 140]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:07', '2017-01-01 1:11', 5, tz='UTC')))

    def test__fetch_ohlcv_dataframe__resample(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol,
                                               since=1483232490000,
                                               limit=3,
                                               timeframe='4m')
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [16, 32, 48],
                    'high': [29, 45, 61],
                    'low': [15, 31, 47],
                    'close': [32, 48, 64],
                    'volume': [472, 536, 600]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:04', '2017-01-01 1:12', 3, tz='UTC')))

    def test__fetch_ohlcv_dataframe__resample_other_freq(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol,
                                               since=1483232590000,
                                               limit=3,
                                               timeframe='3m')
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [24, 36, 48],
                    'high': [33, 45, 57],
                    'low': [23, 35, 47],
                    'close': [36, 48, 60],
                    'volume': [372, 408, 444]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:06', '2017-01-01 1:12', 3, tz='UTC')))

    def test__fetch_ohlcv_dataframe__not_avail_past_values(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(symbol=symbol,
                                               since=1483232330000)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [4, 8, 12],
                    'high': [5, 9, 13],
                    'low': [3, 7, 11],
                    'close': [8, 12, 16],
                    'volume': [100, 104, 108]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 1:01', '2017-01-01 1:03', 3, tz='UTC')))
