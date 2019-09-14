import pandas
import unittest
from ccxt.base.errors import BadRequest, InsufficientFunds, InvalidOrder, \
    OrderNotFound
from decimal import Decimal
from sccts.exchange_backend import Balance, ExchangeBackend
from sccts.backtest import Timeframe


class BalanceTest(unittest.TestCase):

    def test__default_init(self):
        balance = Balance()
        self.assertEqual(balance.free(), 0)
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), 0)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal(0),
            'used': Decimal(0),
            'total': Decimal(0),
        })

    def test__positive_initialization(self):
        balance = Balance(15.3)
        self.assertEqual(balance.free(), Decimal('15.3'))
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), Decimal('15.3'))
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
        self.assertEqual(balance.free(), Decimal('16.0'))
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), Decimal('16.0'))
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('16.0'),
            'used': Decimal(0),
            'total': Decimal('16.0'),
        })

    def test__change_total__substraction(self):
        balance = Balance(15.3)
        balance.change_total(-0.3)
        self.assertEqual(balance.free(), Decimal('15.0'))
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), Decimal('15.0'))
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
        self.assertEqual(balance.free(), Decimal('15.3'))
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), Decimal('15.3'))
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })


class ExchangeBackendTest(unittest.TestCase):

    def setUp(self):
        self.eth_btc_market = {'base': 'ETH', 'quote': 'BTC',
                               'symbol': 'ETH/BTC'}
        self.btc_usd_market = {'base': 'BTC', 'quote': 'USD',
                               'symbol': 'BTC/USD'}
        dates = pandas.to_datetime(['2017-01-01 1:00', '2017-01-01 1:01',
                                    '2017-01-01 1:02'], utc=True)
        self.timeframe = Timeframe(pd_start_date=dates[0],
                                   pd_end_date=dates[-1],
                                   pd_timedelta=pandas.Timedelta(minutes=1))
        self.timeframe.add_timedelta()
        data = {'high': [6, 2, 4],
                'low': [5, 0.5, 1]}
        self.eth_btc_ohlcvs = pandas.DataFrame(data=data, index=dates)

    def test__init__ohlcvs_index_start_bigger_than_start_date(self):
        data = {'high': [6, 2],
                'low': [5, 0.5]}
        dates = pandas.to_datetime(['2017-01-01 1:01', '2017-01-01 1:02'],
                                   utc=True)
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=dates)
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                    'BTC/USD': btc_usd_ohlcvs},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def test__init__ohlcvs_index_end_lower_than_end_date(self):
        data = {'high': [6, 2],
                'low': [5, 0.5]}
        dates = pandas.to_datetime(['2017-01-01 1:00:00', '2017-01-01 1:01'],
                                   utc=True)
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=dates)
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                    'BTC/USD': btc_usd_ohlcvs},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv needs to cover timeframe')

    def test__init__high_missing(self):
        data = {'low': [5, 0.5, 2]}
        dates = pandas.to_datetime(['2017-01-01 1:00:00', '2017-01-01 1:01',
                                    '2017-01-01 1:02'], utc=True)
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=dates)
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                    'BTC/USD': btc_usd_ohlcvs},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv high needs to be provided')

    def test__init__low_missing(self):
        data = {'high': [6, 2, 3]}
        dates = pandas.to_datetime(['2017-01-01 1:00:00', '2017-01-01 1:01',
                                    '2017-01-01 1:02'], utc=True)
        btc_usd_ohlcvs = pandas.DataFrame(data=data, index=dates)
        with self.assertRaises(ValueError) as e:
            ExchangeBackend(timeframe=self.timeframe,
                            ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs,
                                    'BTC/USD': btc_usd_ohlcvs},
                            balances={})
        self.assertEqual(str(e.exception), 'ohlcv low needs to be provided')

    def template__create_order__error(self, exception_text, exception, market,
                                      side, type, amount, price):
        backend = ExchangeBackend(timeframe=self.timeframe,
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

    def test__create_order__unsupported_type(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='limit',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeBackend: only market order supported')

    def test__create_order__market_price_set(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount=5,
            price=1,
            exception=InvalidOrder,
            exception_text='ExchangeBackend: market order has no price')

    def test__create_order__no_market_provided(self):
        self.template__create_order__error(
            market=None,
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=InvalidOrder,
            exception_text='ExchangeBackend: market is None')

    def test__create_order__has_no_prices(self):
        self.template__create_order__error(
            market=self.btc_usd_market,
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=InvalidOrder,
            exception_text='ExchangeBackend: no prices available for BTC/USD')

    def test__create_order__unsupported_side(self):
        self.template__create_order__error(
            market=self.btc_usd_market,
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=InvalidOrder,
            exception_text='ExchangeBackend: no prices available for BTC/USD')

    def test__create_order__market_has_no_quote(self):
        self.template__create_order__error(
            market={'base': 'ETH', 'symbol': 'ETH/BTC'},
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: market has no quote')

    def test__create_order__market_has_no_base(self):
        self.template__create_order__error(
            market={'quote': 'ETH', 'symbol': 'ETH/BTC'},
            side='buy',
            type='market',
            amount=5,
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: market has no base')

    def test__create_order__amount_not_finite(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount=float('inf'),
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: amount needs to be finite')

    def test__create_order__amount_not_a_number(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount='wrong number',
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: amount needs to be a number')

    def test__create_order__amount_is_zero(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount=0,
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: amount needs to be positive')

    def test__create_order__less_than_zero(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount=-20,
            price=None,
            exception=BadRequest,
            exception_text='ExchangeBackend: amount needs to be positive')

    def test__create_order__market_buy__insufficient_funds(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='buy',
            type='market',
            amount=20,
            price=None,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__market_sell__insufficient_funds(self):
        self.template__create_order__error(
            market=self.eth_btc_market,
            side='sell',
            type='market',
            amount=20000,
            price=None,
            exception=InsufficientFunds,
            exception_text='Balance too little')

    def test__create_order__market_buy__create_balance(self):
        backend = ExchangeBackend(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 3})
        backend.create_order(market=self.eth_btc_market, side='buy',
                             type='market', amount=1, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 0.997, 'total': 0.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_buy__balance_available(self):
        backend = ExchangeBackend(timeframe=self.timeframe,
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
        backend = ExchangeBackend(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        backend.create_order(market=self.eth_btc_market, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 0.9985, 'total': 0.9985,
                                  'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__create_order__market_sell__balance_available(self):
        backend = ExchangeBackend(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'ETH': 3})
        self.timeframe.add_timedelta()
        backend.create_order(market=self.eth_btc_market, side='sell',
                             type='market', amount=2, price=None)
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 1.997, 'total': 1.997, 'used': 0.0},
                          'ETH': {'free': 1.0, 'total': 1.0, 'used': 0.0}})

    def test__fetch_balance(self):
        backend = ExchangeBackend(timeframe=None,
                                  balances={'BTC': 15.3,
                                            'USD': 0.3})
        self.assertEqual(backend.fetch_balance(),
                         {'BTC': {'free': 15.3, 'total': 15.3, 'used': 0.0},
                          'USD': {'free': 0.3, 'total': 0.3, 'used': 0.0}})

    def test__fetch_order(self):
        backend = ExchangeBackend(timeframe=self.timeframe,
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
        backend = ExchangeBackend(timeframe=self.timeframe,
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
        backend = ExchangeBackend(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            backend.fetch_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeBackend: order some_id does not exist')

    def test__cancel_order__market(self):
        backend = ExchangeBackend(timeframe=self.timeframe,
                                  ohlcvs={'ETH/BTC': self.eth_btc_ohlcvs},
                                  balances={'BTC': 7,
                                            'ETH': 2})
        buy_id = backend.create_order(market=self.eth_btc_market, side='buy',
                                      type='market', amount=1, price=None)
        with self.assertRaises(BadRequest) as e:
            backend.cancel_order(buy_id['id'])
        self.assertEqual(str(e.exception),
                         'ExchangeBackend: cannot cancel market order')

    def test__cancel_order__not_found(self):
        backend = ExchangeBackend(timeframe=self.timeframe)
        with self.assertRaises(OrderNotFound) as e:
            backend.cancel_order('some_id')
        self.assertEqual(str(e.exception),
                         'ExchangeBackend: order some_id does not exist')
