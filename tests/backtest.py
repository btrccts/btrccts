import ccxt
import unittest
import pandas
from unittest.mock import patch
from sccts.backtest import Backtest, Timeframe
from sccts.exchange import BacktestExchangeBase
from sccts.exchange_backend import ExchangeBackend


def pd_ts(s):
    return pandas.Timestamp(s, tz='UTC')


class BacktestTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        backtest = Backtest(timeframe=None)
        with self.assertRaises(ValueError) as e:
            backtest.create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    @patch('sccts.backtest.BacktestExchangeBase.__init__')
    def test__create_exchange__parameters(self, base_init_mock):
        base_init_mock.return_value = None
        bitfinex_backend = ExchangeBackend(timeframe=None)
        binance_backend = ExchangeBackend(timeframe=None)
        backtest = Backtest(timeframe=None,
                            exchange_backends={'bitfinex': bitfinex_backend,
                                               'binance': binance_backend})
        exchange = backtest.create_exchange('bitfinex', {'parameter': 123})
        base_init_mock.assert_called_once_with(
            config={'parameter': 123},
            exchange_backend=bitfinex_backend)
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.bitfinex))

    @patch('sccts.backtest.ExchangeBackend')
    @patch('sccts.backtest.BacktestExchangeBase.__init__')
    def test__create_exchange__default_exchange_backend_parameters(
            self, base_init_mock, exchange_backend):
        base_init_mock.return_value = None
        timeframe = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                              pd_end_date=pd_ts('2017-01-01 1:03'),
                              pd_timedelta=pandas.Timedelta(minutes=1))
        backtest = Backtest(timeframe=timeframe)
        exchange = backtest.create_exchange('binance', {'some': 'test'})
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.binance))
        exchange_backend.assert_called_once_with(
            timeframe=timeframe)
        base_init_mock.assert_called_once_with(
            config={'some': 'test'},
            exchange_backend=exchange_backend())

    def test__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        backtest = Backtest(timeframe=t)
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:01'))


class TimeframeTest(unittest.TestCase):

    def test__end_date_smaller_than_start_date(self):
        with self.assertRaises(ValueError) as e:
            Timeframe(pd_start_date=pd_ts('2017-02-01'),
                      pd_end_date=pd_ts('2017-01-01'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        self.assertEqual(str(e.exception),
                         'Timeframe: end date is smaller then start date')

    def test__add_timedelta__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:03'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:00'))
        # should return the same value
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:01'))
        t.add_timedelta()
        t.add_timedelta()
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:03'))
        t.add_timedelta()
        self.assertEqual(t.date(), None)

    def test__different_timedelta(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_timedelta=pandas.Timedelta(minutes=15))
        t.add_timedelta()
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:15'))
        t.add_timedelta()
        self.assertEqual(t.date(), pd_ts('2017-01-01 1:30'))
        t.add_timedelta()
        self.assertEqual(t.date(), None)
