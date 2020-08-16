import pandas
import unittest
from ccxt.base.errors import BadRequest, BadSymbol
from unittest.mock import patch, MagicMock
from btrccts.exchange_backend import ExchangeBackend
from btrccts.timeframe import Timeframe


class ExchangeBackendTest(unittest.TestCase):

    def setUp(self):
        dates = pandas.to_datetime(['2017-01-01 1:00', '2017-01-01 1:01',
                                    '2017-01-01 1:02'], utc=True)
        self.init_timeframe = Timeframe(pd_start_date=dates[0],
                                        pd_end_date=dates[-1],
                                        pd_interval=pandas.Timedelta(
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
            pd_interval=pandas.Timedelta(minutes=1))
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

    @patch("btrccts.exchange_backend.ExchangeAccount")
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

    @patch("btrccts.exchange_backend.ExchangeAccount")
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

    def test__fetch_closed_orders(self):
        self.template_exchange_account_method_propagated(
            kwargs={'symbol': 'BTC/ETH', 'since': 0, 'limit': 15},
            methodname='fetch_closed_orders')

    def test__fetch_open_orders(self):
        self.template_exchange_account_method_propagated(
            kwargs={'symbol': 'BTC/ETH', 'since': 110, 'limit': 150},
            methodname='fetch_open_orders')

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
                symbol=symbol, timeframe='1m', since=1483232550000, limit=18)
        self.assertEqual(
            str(e.exception),
            'ExchangeBackend: fetch_ohlcv: since.ceil(timeframe) + limit'
            ' * timeframe needs to be in the past')

    def test__fetch_ohlcv_dataframe__access_future__timeframe(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        with self.assertRaises(BadRequest) as e:
            backend.fetch_ohlcv_dataframe(
                symbol=symbol, timeframe='2m', since=1483232610000, limit=10)
        self.assertEqual(
            str(e.exception),
            'ExchangeBackend: fetch_ohlcv: since.ceil(timeframe) + limit'
            ' * timeframe needs to be in the past')

    def test__fetch_ohlcv_dataframe__partial_data_agg(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(
            symbol=symbol, timeframe='6m', since=1483232610000, limit=3)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [24.0, 48.0, 72.0],
                    'high': [45.0, 69.0, 77.0],
                    'low': [23.0, 47.0, 71.0],
                    'close': [48.0, 72.0, 80.0],
                    'volume': [780.0, 924.0, 340.0]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 01:06', '2017-01-01 01:18',
                    tz='UTC', freq='6T')))

    def test__fetch_ohlcv_dataframe__partial_data(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        result = backend.fetch_ohlcv_dataframe(
            symbol=symbol, timeframe='3m', since=1483233000000, limit=3)
        pandas.testing.assert_frame_equal(
            result,
            pandas.DataFrame(
                data={
                    'open': [48.0, 60.0, 72.0],
                    'high': [57.0, 69.0, 77.0],
                    'low': [47.0, 59.0, 71.0],
                    'close': [60.0, 72.0, 80.0],
                    'volume': [444.0, 480.0, 340.0]},
                dtype=float,
                index=pandas.date_range(
                    '2017-01-01 01:12', '2017-01-01 01:18',
                    freq='3T', tz='UTC')))

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
                    '2017-01-01 1:01', '2017-01-01 1:05',
                    freq='1T', tz='UTC')))

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
                    '2017-01-01 1:01', '2017-01-01 1:03',
                    freq='1T', tz='UTC')))

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
                    '2017-01-01 1:07', '2017-01-01 1:11',
                    freq='1T', tz='UTC')))

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
                    '2017-01-01 1:04', '2017-01-01 1:12',
                    freq='4T', tz='UTC')))

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
                    '2017-01-01 1:06', '2017-01-01 1:12',
                    freq='3T', tz='UTC')))

    def test__fetch_ohlcv_dataframe__not_avail_past_values(self):
        symbol = 'BTC/USD'
        backend = ExchangeBackend(ohlcvs={symbol: self.fetch_ohlcv_ohlcvs},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        with self.assertRaises(BadRequest) as e:
            backend.fetch_ohlcv_dataframe(symbol=symbol,
                                          since=1483232330000)
        self.assertEqual(str(e.exception), 'ExchangeBackend: fetch_ohlcv: no '
                                           'date availabe at since')

    def test__fetch_ticker(self):
        timeframe = Timeframe(pd_start_date=self.fetch_ohlcv_ohlcvs.index[0],
                              pd_end_date=self.fetch_ohlcv_ohlcvs.index[-1],
                              pd_interval=pandas.Timedelta(minutes=0.5))
        backend = ExchangeBackend(ohlcvs={'BTC/USD': self.fetch_ohlcv_ohlcvs},
                                  timeframe=timeframe,
                                  balances={})
        timeframe.add_timedelta()
        timeframe.add_timedelta()
        timeframe.add_timedelta()
        self.assertEqual(backend.fetch_ticker('BTC/USD'),
                         {'symbol': 'BTC/USD',
                          'timestamp': 1483232520000,
                          'datetime': '2017-01-01T01:02:00.000Z',
                          'high': 9.0,
                          'low': 7.0,
                          'bid': None,
                          'bidVolume': None,
                          'ask': None,
                          'askVolume': None,
                          'vwap': None,
                          'open': 8.0,
                          'close': 12.0,
                          'last': None,
                          'previousClose': None,
                          'change': None,
                          'percentage': None,
                          'average': None,
                          'baseVolume': None,
                          'quoteVolume': None,
                          'info': {}})

    def test__fetch_ticker__exception(self):
        backend = ExchangeBackend(ohlcvs={},
                                  timeframe=self.fetch_ohlcv_timeframe,
                                  balances={})
        with self.assertRaises(BadSymbol) as e:
            backend.fetch_ticker('BTC/USD')
        self.assertEqual(str(e.exception),
                         'ExchangeBackend: no prices for BTC/USD')
