import ccxt
import unittest
import pandas
from unittest.mock import patch, call
from btrccts.timeframe import Timeframe
from btrccts.context import BacktestContext, ContextState, LiveContext, \
    StopException
from btrccts.exchange import BacktestExchangeBase
from btrccts.async_exchange import AsyncBacktestExchangeBase
from btrccts.exchange_backend import ExchangeBackend
from tests.common import pd_ts, async_test


class BacktestContextTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        backtest = BacktestContext(timeframe=None)
        with self.assertRaises(ValueError) as e:
            backtest.create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    @patch('btrccts.context.BacktestExchangeBase.__init__')
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

    @patch('btrccts.context.ExchangeBackend')
    @patch('btrccts.context.BacktestExchangeBase.__init__')
    @patch('btrccts.context.AsyncBacktestExchangeBase.__init__')
    @async_test
    async def test__create_exchange__default_exchange_backend_parameters(
            self, async_init_mock, base_init_mock, exchange_backend):
        base_init_mock.return_value = None
        async_init_mock.return_value = None
        timeframe = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                              pd_end_date=pd_ts('2017-01-01 1:03'),
                              pd_interval=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=timeframe)
        # Create two instances, to see if they get the same backend
        backtest.create_exchange('binance', {'some': 'test'}, async_ccxt=True)
        async_init_mock.assert_called_once_with(
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
                  exchange_backend=exchange_backend.return_value)])
        self.assertEqual(
            async_init_mock.mock_calls,
            [call(config={'some': 'test'},
                  exchange_backend=exchange_backend.return_value)])

    def test__create_exchange__not_an_async_exchange(self):
        backtest = BacktestContext(timeframe=None)
        with self.assertRaises(ValueError) as e:
            backtest.create_exchange('not_an_exchange', async_ccxt=True)
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    @patch('btrccts.context.AsyncBacktestExchangeBase.__init__')
    @async_test
    async def test__create_exchange__async__parameters(self, base_init_mock):
        base_init_mock.return_value = None
        bitfinex_backend = ExchangeBackend(timeframe=None)
        binance_backend = ExchangeBackend(timeframe=None)
        backtest = BacktestContext(timeframe=None,
                                   exchange_backends={
                                       'bitfinex': bitfinex_backend,
                                       'binance': binance_backend})
        exchange = backtest.create_exchange('bitfinex', {'parameter': 123},
                                            async_ccxt=True)
        base_init_mock.assert_called_once_with(
            config={'parameter': 123},
            exchange_backend=bitfinex_backend)
        self.assertEqual(exchange.__class__.__bases__,
                         (AsyncBacktestExchangeBase,
                          ccxt.async_support.bitfinex))

    def test__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_interval=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=t)
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.date(), pd_ts('2017-01-01 1:01'))

    def test__real_date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_interval=pandas.Timedelta(minutes=1))
        backtest = BacktestContext(timeframe=t)
        self.assertEqual(backtest.real_date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(backtest.real_date(), pd_ts('2017-01-01 1:01'))

    def test__state(self):
        backtest = BacktestContext(timeframe=None)
        self.assertEqual(backtest.state(), ContextState.BACKTEST)

    def test__stop(self):
        context = LiveContext(timeframe=None, conf_dir='')
        with self.assertRaises(StopException) as e:
            context.stop('msg')
        self.assertEqual(str(e.exception), 'msg')


class CCXTProBinance:

    def __init__(self, config={}):
        pass


class CCXTProBitfinex:

    def __init__(self, config={}):
        pass


class CCXTProMock:

    def __init__(self):
        self.exchanges = ['binance', 'bitfinex']
        self.bitfinex = CCXTProBinance
        self.binance = CCXTProBitfinex


class LiveContextTest(unittest.TestCase):

    def create_exchange__not_an_exchange__template(self, async_ccxt):
        context = LiveContext(timeframe=None, auth_aliases={}, conf_dir='')
        with self.assertRaises(ValueError) as e:
            context.create_exchange('not_an_exchange', async_ccxt=async_ccxt)
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    def create_exchange__template(self, base_init_mock, instance, async_ccxt):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None,
                              conf_dir='tests/unit/context/config',
                              auth_aliases={})
        exchange = context.create_exchange(
            'bitfinex', {'parameter': 2}, async_ccxt=async_ccxt)
        base_init_mock.assert_called_once_with(
            {'apiKey': '555',
             'enableRateLimit': True,
             'parameter': 2})
        self.assertTrue(isinstance(exchange, instance))

    def create_exchange__no_file__template(self, base_init_mock, instance,
                                           async_ccxt):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None, conf_dir='/dir',
                              auth_aliases={'binance': 'bb'})
        with self.assertLogs('btrccts') as cm:
            exchange = context.create_exchange(
                'binance', {'parameter': 123}, async_ccxt=async_ccxt)
        self.assertEqual(
            cm.output,
            ['WARNING:btrccts:Config file for exchange binance does not'
             ' exist: /dir/bb.json'])
        base_init_mock.assert_called_once_with({'parameter': 123,
                                                'enableRateLimit': True})
        self.assertTrue(isinstance(exchange, instance))

    def create_exchange__merge_config__template(self, base_init_mock,
                                                instance, async_ccxt):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None,
                              conf_dir='tests/unit/context/config',
                              auth_aliases={'binance': 'binance_mod'})
        exchange = context.create_exchange(
            'binance', {'parameter': 2, 'enableRateLimit': False},
            async_ccxt=async_ccxt)
        base_init_mock.assert_called_once_with(
            {'apiKey': 'testkey',
             'secret': 'secret',
             'other': True,
             'enableRateLimit': False,
             'parameter': 2})
        self.assertTrue(isinstance(exchange, instance))

    def test__create_exchange__not_an_exchange(self):
        self.create_exchange__not_an_exchange__template(False)

    @patch('ccxt.binance.__init__')
    def test__create_exchange__no_file(self, base_init_mock):
        self.create_exchange__no_file__template(
            base_init_mock, ccxt.binance, False)

    @patch('ccxt.bitfinex.__init__')
    def test__create_exchange(self, base_init_mock):
        self.create_exchange__template(base_init_mock, ccxt.bitfinex, False)

    @patch('ccxt.binance.__init__')
    def test__create_exchange__merge_config(self, base_init_mock):
        self.create_exchange__merge_config__template(
            base_init_mock, ccxt.binance, False)

    def test__create_exchange__not_an_async_exchange(self):
        self.create_exchange__not_an_exchange__template(True)

    @patch('ccxt.async_support.bitfinex.__init__')
    def test__create_exchange__async(self, base_init_mock):
        self.create_exchange__template(
            base_init_mock, ccxt.async_support.bitfinex, True)

    @patch('ccxt.async_support.binance.__init__')
    def test__create_exchange__async_no_file(self, base_init_mock):
        self.create_exchange__no_file__template(
            base_init_mock, ccxt.async_support.binance, True)

    @patch('ccxt.async_support.binance.__init__')
    def test__create_exchange__async_merge_config(self, base_init_mock):
        self.create_exchange__merge_config__template(
            base_init_mock, ccxt.async_support.binance, True)

    @patch('btrccts.context.ccxtpro', CCXTProMock())
    def test__create_exchange__not_an_ccxtpro_exchange(self):
        self.create_exchange__not_an_exchange__template(True)

    @patch('btrccts.context.ccxtpro', CCXTProMock())
    @patch('tests.unit.context.CCXTProBinance.__init__')
    def test__create_exchange__ccxtpro(self, base_init_mock):
        self.create_exchange__template(
            base_init_mock, CCXTProBinance, True)

    @patch('btrccts.context.ccxtpro', CCXTProMock())
    @patch('tests.unit.context.CCXTProBitfinex.__init__')
    def test__create_exchange__ccxtpro_no_file(self, base_init_mock):
        self.create_exchange__no_file__template(
            base_init_mock, CCXTProBitfinex, True)

    @patch('btrccts.context.ccxtpro', CCXTProMock())
    @patch('tests.unit.context.CCXTProBitfinex.__init__')
    def test__create_exchange__ccxtpro_merge_config(self, base_init_mock):
        self.create_exchange__merge_config__template(
            base_init_mock, CCXTProBitfinex, True)

    @patch('btrccts.context.ccxtpro', CCXTProMock())
    @patch('ccxt.async_support.bithumb.__init__')
    def test__create_exchange__not_in_ccxtpro(self, base_init_mock):
        base_init_mock.return_value = None
        context = LiveContext(timeframe=None,
                              conf_dir='tests/unit/context/config',
                              auth_aliases={})
        exchange = context.create_exchange('bithumb', {'param': 2}, True)
        base_init_mock.assert_called_once_with(
            {'apiKey': 'abc',
             'enableRateLimit': True,
             'param': 2})
        self.assertTrue(isinstance(exchange, ccxt.async_support.bithumb))

    def test__date(self):
        t = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                      pd_end_date=pd_ts('2017-01-01 1:35'),
                      pd_interval=pandas.Timedelta(minutes=1))
        context = LiveContext(timeframe=t, conf_dir='')
        self.assertEqual(context.date(), pd_ts('2017-01-01 1:00'))
        t.add_timedelta()
        self.assertEqual(context.date(), pd_ts('2017-01-01 1:01'))

    @patch('btrccts.context.pandas.Timestamp.now')
    def test__real_date(self, now_mock):
        context = LiveContext(timeframe=None, conf_dir='')
        result = context.real_date()
        now_mock.assert_called_once_with(tz='UTC')
        self.assertEqual(result, now_mock())

    def test__state(self):
        context = LiveContext(timeframe=None, conf_dir='')
        self.assertEqual(context.state(), ContextState.LIVE)

    def test__stop(self):
        context = LiveContext(timeframe=None, conf_dir='')
        with self.assertRaises(StopException) as e:
            context.stop('stop')
        self.assertEqual(str(e.exception), 'stop')
