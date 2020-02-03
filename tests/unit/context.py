import ccxt
import unittest
import pandas
from unittest.mock import patch, call
from sccts.timeframe import Timeframe
from sccts.context import BacktestContext, ContextState, LiveContext
from sccts.exchange import BacktestExchangeBase
from sccts.exchange_backend import ExchangeBackend
from tests.common import pd_ts


class BacktestContextTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        backtest = BacktestContext(timeframe=None)
        with self.assertRaises(ValueError) as e:
            backtest.create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    @patch('sccts.context.BacktestExchangeBase.__init__')
    def test__create_exchange__parameters(self, base_init_mock):
        base_init_mock.return_value = None
        bitfinex_backend = ExchangeBackend(timeframe=None)
        binance_backend = ExchangeBackend(timeframe=None)
        backtest = BacktestContext(timeframe=None,
                                   exchange_backends={
                                       'bitfinex': bitfinex_backend,
                                       'binance': binance_backend})
        exchange = backtest.create_exchange('bitfinex', {'parameter': 123})
        base_init_mock.assert_called_once_with(
            config={'parameter': 123},
            exchange_backend=bitfinex_backend)
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.bitfinex))

    @patch('sccts.context.ExchangeBackend')
    @patch('sccts.context.BacktestExchangeBase.__init__')
    def test__create_exchange__default_exchange_backend_parameters(
            self, base_init_mock, exchange_backend):
        base_init_mock.return_value = None
        timeframe = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                              pd_end_date=pd_ts('2017-01-01 1:03'),
                              pd_timedelta=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=timeframe)
        # Create two instances, to see if they get the same backend
        backtest.create_exchange('binance', {'some': 'test'})
        base_init_mock.assert_called_once_with(
            config={'some': 'test'},
            exchange_backend=exchange_backend.return_value)
        exchange = backtest.create_exchange('binance', {'some': 'test'})
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.binance))
        exchange_backend.assert_called_once_with(
            timeframe=timeframe)
        self.assertEqual(
            base_init_mock.mock_calls,
            [call(config={'some': 'test'},
                  exchange_backend=exchange_backend.return_value),
             call(config={'some': 'test'},
                  exchange_backend=exchange_backend.return_value)])

    def test__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=t)
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:01'))

    def test__real_date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=t)
        self.assertEqual(backtest.real_date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.real_date(), pd_ts('2017-01-01 1:01'))

    def test__state(self):
        backtest = BacktestContext(timeframe=None)
        self.assertEqual(backtest.state(), ContextState.BACKTEST)

    def test__stop__stopped(self):
        backtest = BacktestContext(timeframe=None)
        self.assertEqual(backtest.stopped(), False)
        backtest.stop()
        self.assertEqual(backtest.stopped(), True)


class LiveContextTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        context = LiveContext(timeframe=None, auth_aliases={}, conf_dir='')
        with self.assertRaises(ValueError) as e:
            context.create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    @patch('ccxt.bitfinex.__init__')
    def test__create_exchange(self, base_init_mock):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None,
                              conf_dir='tests/unit/context/config',
                              auth_aliases={})
        exchange = context.create_exchange('bitfinex', {'parameter': 2})
        base_init_mock.assert_called_once_with(
            {'apiKey': '555',
             'parameter': 2})
        self.assertTrue(isinstance(exchange, ccxt.bitfinex))

    @patch('ccxt.binance.__init__')
    def test__create_exchange__no_file(self, base_init_mock):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None, conf_dir='/dir',
                              auth_aliases={'binance': 'bb'})
        with self.assertLogs('sccts') as cm:
            exchange = context.create_exchange('binance', {'parameter': 123})
        self.assertEqual(
            cm.output,
            ['WARNING:sccts:Config file for exchange binance does not'
             ' exist: /dir/bb.json'])
        base_init_mock.assert_called_once_with({'parameter': 123})
        self.assertTrue(isinstance(exchange, ccxt.binance))

    @patch('ccxt.binance.__init__')
    def test__create_exchange__merge_config(self, base_init_mock):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None,
                              conf_dir='tests/unit/context/config',
                              auth_aliases={'binance': 'binance_mod'})
        exchange = context.create_exchange('binance', {'parameter': 2})
        base_init_mock.assert_called_once_with(
            {'apiKey': 'testkey',
             'secret': 'secret',
             'other': True,
             'parameter': 2})
        self.assertTrue(isinstance(exchange, ccxt.binance))

    def test__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_timedelta=pandas.Timedelta(minutes=1))
        context = LiveContext(timeframe=t, conf_dir='')
        self.assertEqual(context.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(context.date(), pd_ts('2017-01-01 1:01'))

    @patch('sccts.context.pandas.Timestamp.now')
    def test__real_date(self, now_mock):
        context = LiveContext(timeframe=None, conf_dir='')
        result = context.real_date()
        now_mock.assert_called_once_with(tz='UTC')
        self.assertEqual(result, now_mock())

    def test__state(self):
        context = LiveContext(timeframe=None, conf_dir='')
        self.assertEqual(context.state(), ContextState.LIVE)

    def test__stop__stopped(self):
        context = LiveContext(timeframe=None, conf_dir='')
        self.assertEqual(context.stopped(), False)
        context.stop()
        self.assertEqual(context.stopped(), True)
