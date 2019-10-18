import ccxt
import unittest
import pandas
from unittest.mock import patch
from sccts.timeframe import Timeframe
from sccts.context import BacktestContext, ContextState
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
        backtest = BacktestContext(timeframe=t)
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:01'))

    def test__state(self):
        backtest = BacktestContext(timeframe=None)
        self.assertEqual(backtest.state(), ContextState.BACKTEST)
