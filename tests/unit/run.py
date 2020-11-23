import asyncio
import os
import pandas
import sys
import unittest
from btrccts.algorithm import AlgorithmBase, AlgorithmBaseSync
from btrccts.run import load_ohlcvs, main_loop, ExitReason, \
    execute_algorithm, parse_params_and_execute_algorithm, sleep_until, \
    StopException
from btrccts.timeframe import Timeframe
from unittest.mock import Mock, call, patch
from tests.common_algos import TestAlgo, assert_test_algo_result, AsyncTestAlgo
from tests.common import fetch_markets_return, BTC_USD_MARKET, ETH_BTC_MARKET,\
    pd_ts, async_test, async_noop, async_fetch_markets_return, async_return

here = os.path.dirname(__file__)
data_dir = os.path.join(here, 'run', 'data_dir')
config_dir = os.path.join(here, 'run', 'config_dir')
ohlcv_dir = os.path.join(data_dir, 'ohlcv')

binance_eth_btc = pandas.DataFrame(
    index=pandas.to_datetime(['2017-08-18 00:00:00', '2017-08-18 00:01:00',
                              '2017-08-18 00:02:00', '2017-08-18 00:03:00'],
                             utc=True),
    data={'open': [4285.08, 4285.08, 4285.08, 4285.08],
          'high': [4285.08, 4285.08, 4285.08, 4287.09],
          'low': [4285.08, 4285.08, 4285.08, 4285.08],
          'close': [4285.08, 4285.08, 4285.08, 4287.09],
          'volume': [0.022196, 0.463420, 0.215048, 0.926866]})
binance_xrp_eth = pandas.DataFrame(
    index=pandas.to_datetime(['2017-08-18 00:43:00', '2017-08-18 00:44:00'],
                             utc=True),
    data={'open': [4307.72, 4307.72],
          'high': [4307.72, 4307.72],
          'low': [4307.72, 4302.15],
          'close': [4307.72, 4302.15],
          'volume': [0.458212, 0.397427]})
bitmex_eth_btc = pandas.DataFrame(
    index=pandas.to_datetime(['2017-10-03 09:57:00', '2017-10-03 09:58:00',
                              '2017-10-03 09:59:00', '2017-10-03 10:00:00'],
                             utc=True),
    data={'open': [4260.64, 4260.65, 4271.99, 4271.97],
          'high': [4260.65, 4260.65, 4271.99, 4271.97],
          'low': [4260.64, 4260.65, 4261.05, 4258.67],
          'close': [4260.65, 4260.65, 4261.05, 4258.67],
          'volume': [0.409203, 0.365193, 0.491765, 0.325784]})
bitmex_xrp_eth = pandas.DataFrame(
    index=pandas.to_datetime(['2017-12-31 23:07', '2017-12-31 23:08',
                              '2017-12-31 23:09'], utc=True),
    data={'open': [13775.0, 13750.1, 13750.1],
          'high': [13775.0, 13766.8, 13766.8],
          'low': [13750.10, 13718.87, 13736.30],
          'close': [13750.10, 13766.80, 13763.85],
          'volume': [3.088494, 6.079897, 3.522711]})


class LoadCSVTests(unittest.TestCase):

    def assert_frame_equal(self, d1, d2):
        pandas.testing.assert_frame_equal(d1.sort_index(axis=1),
                                          d2.sort_index(axis=1))

    def test__load_ohlcvs__all_symbols_no_exchange_directory(self):
        with self.assertRaises(FileNotFoundError) as e:
            load_ohlcvs(ohlcv_dir=ohlcv_dir,
                        exchange_names=['acx'],
                        symbols=[])
        self.assertEqual(
            str(e.exception),
            'Cannot find ohlcv directory for exchange (acx)')

    def test__load_ohlcvs__symbol_file_does_not_exist(self):
        with self.assertRaises(FileNotFoundError) as e:
            load_ohlcvs(ohlcv_dir=ohlcv_dir,
                        exchange_names=['bittrex'],
                        symbols=['ETH/BTC'])
        self.assertEqual(
            str(e.exception),
            'Cannot find symbol (ETH/BTC) file for exchange (bittrex)')

    def test__load_ohlcvs__exchange_does_not_exist(self):
        with self.assertRaises(FileNotFoundError) as e:
            load_ohlcvs(ohlcv_dir=ohlcv_dir,
                        exchange_names=['inexistent'],
                        symbols=['ETH/BTC'])
        self.assertEqual(
            str(e.exception),
            'Cannot find symbol (ETH/BTC) file for exchange (inexistent)')

    def test__load_ohlcvs__defect_file(self):
        with self.assertRaises(ValueError) as e:
            load_ohlcvs(ohlcv_dir=ohlcv_dir,
                        exchange_names=['defect'],
                        symbols=['XRP/ETH'])
        self.assertEqual(
            str(e.exception),
            'Cannot parse symbol (XRP/ETH) file for exchange (defect)')

    def test__load_ohlcvs(self):
        result = load_ohlcvs(ohlcv_dir=ohlcv_dir,
                             exchange_names=['bitmex', 'binance'],
                             symbols=['ETH/BTC', 'XRP/ETH'])
        self.assertEqual(sorted(result.keys()), ['binance', 'bitmex'])
        self.assertEqual(sorted(result['binance'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assertEqual(sorted(result['bitmex'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assert_frame_equal(result['binance']['ETH/BTC'], binance_eth_btc)
        self.assert_frame_equal(result['binance']['XRP/ETH'], binance_xrp_eth)
        self.assert_frame_equal(result['bitmex']['XRP/ETH'], bitmex_xrp_eth)
        self.assert_frame_equal(result['bitmex']['ETH/BTC'], bitmex_eth_btc)

    def test__load_ohlcvs__all_symbols_per_exchange(self):
        result = load_ohlcvs(ohlcv_dir=ohlcv_dir,
                             exchange_names=['bitmex'],
                             symbols=[])
        self.assertEqual(sorted(result.keys()), ['bitmex'])
        self.assertEqual(sorted(result['bitmex'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assert_frame_equal(result['bitmex']['XRP/ETH'], bitmex_xrp_eth)
        self.assert_frame_equal(result['bitmex']['ETH/BTC'], bitmex_eth_btc)


class MainLoopTests(unittest.TestCase):

    def algo(self, algorithm):
        return algorithm

    def setUp(self):
        self.timeframe = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                                   pd_end_date=pd_ts('2017-01-01 1:03'),
                                   pd_interval=pandas.Timedelta(minutes=1))

    @async_test
    async def test__main_loop__successful(self):
        algorithm = Mock(spec=AlgorithmBaseSync)
        error = ValueError('a')
        algorithm.next_iteration.side_effect = [0, 0, error, 0]
        use_algorithm = self.algo(algorithm)
        with self.assertLogs('btrccts') as cm:
            result = await main_loop(timeframe=self.timeframe,
                                     algorithm=use_algorithm)
        self.assertEqual(result, use_algorithm)
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.next_iteration(),
                          call.handle_exception(error),
                          call.next_iteration(),
                          call.exit(reason=ExitReason.FINISHED)])
        self.assertEqual(len(cm.output), 4)
        self.assertEqual(cm.output[0:2],
                         ['INFO:btrccts:Starting main_loop',
                          'ERROR:btrccts:Error occured during next_iteration'])
        self.assertTrue(cm.output[2].startswith(
            'ERROR:btrccts:a\nTraceback (most recent call last):\n  File'))
        self.assertEqual(cm.output[3], 'INFO:btrccts:Finished main_loop')

    @async_test
    async def test__main_loop__handle_exception_throws(self):
        algorithm = Mock(spec=AlgorithmBaseSync)
        error = ValueError('a')
        algorithm.next_iteration.side_effect = [0, error, 0, 0]
        algorithm.handle_exception.side_effect = AttributeError('side')
        use_algorithm = self.algo(algorithm)
        with self.assertLogs('btrccts') as cm:
            with self.assertRaises(AttributeError) as e:
                await main_loop(
                    timeframe=self.timeframe, algorithm=use_algorithm)
        self.assertEqual(str(e.exception), 'side')
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.handle_exception(error),
                          call.exit(reason=ExitReason.EXCEPTION)])
        self.assertEqual(len(cm.output), 5)
        self.assertEqual(cm.output[0:2],
                         ['INFO:btrccts:Starting main_loop',
                          'ERROR:btrccts:Error occured during next_iteration'])
        self.assertTrue(cm.output[2].startswith(
            'ERROR:btrccts:a\nTraceback (most recent call last):\n  File'))
        self.assertEqual(cm.output[3], 'ERROR:btrccts:Exiting because of '
                                       'exception in handle_exception')
        self.assertTrue(cm.output[4].startswith(
            'ERROR:btrccts:side\nTraceback (most recent call last):\n  File'))

    @async_test
    async def template__main_loop__exit_exception(self, exception_class,
                                                  log_str):
        algorithm = Mock(spec=AlgorithmBaseSync)
        algorithm.next_iteration.side_effect = [0, exception_class('aa'), 0, 0]
        use_algorithm = self.algo(algorithm)
        with self.assertLogs('btrccts') as cm:
            result = await main_loop(timeframe=self.timeframe,
                                     algorithm=use_algorithm)
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.exit(reason=ExitReason.STOPPED)])
        self.assertEqual(cm.output,
                         ['INFO:btrccts:Starting main_loop', log_str])
        self.assertEqual(use_algorithm, result)

    def test__main_loop__systemexit(self):
        self.template__main_loop__exit_exception(
            SystemExit, 'INFO:btrccts:Stopped because of SystemExit: aa')

    def test__main_loop__keyboardinterrupt(self):
        self.template__main_loop__exit_exception(
            KeyboardInterrupt,
            'INFO:btrccts:Stopped because of KeyboardInterrupt: aa')

    def test__main_loop__cancellederror(self):
        self.template__main_loop__exit_exception(
            asyncio.CancelledError,
            'INFO:btrccts:Stopped because of CancelledError: aa')

    def test__main_loop__stop(self):
        self.template__main_loop__exit_exception(
            StopException,
            'INFO:btrccts:Stopped because of StopException: aa')

    @patch('btrccts.run.asyncio.sleep')
    @async_test
    async def template__main_loop__exit_exception_during_sleep(
            self, exception_class, log_str, sleep_mock):
        algorithm = Mock(spec=AlgorithmBaseSync)
        sleep_mock.side_effect = [exception_class('aa')]
        # We need to use future dates, because we are in live mode
        timeframe = Timeframe(pd_start_date=pd_ts('2217-01-01 1:00'),
                              pd_end_date=pd_ts('2217-01-01 1:03'),
                              pd_interval=pandas.Timedelta(minutes=1))
        use_algorithm = self.algo(algorithm)
        with self.assertLogs('btrccts') as cm:
            result = await main_loop(timeframe=timeframe,
                                     algorithm=use_algorithm,
                                     live=True)
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.exit(reason=ExitReason.STOPPED)])
        self.assertEqual(cm.output,
                         ['INFO:btrccts:Starting main_loop', log_str])
        self.assertEqual(use_algorithm, result)

    def test__main_loop__systemexit_in_sleep(self):
        self.template__main_loop__exit_exception_during_sleep(
            SystemExit, 'INFO:btrccts:Stopped because of SystemExit: aa')

    def test__main_loop__keyboardinterrupt_in_sleep(self):
        self.template__main_loop__exit_exception_during_sleep(
            KeyboardInterrupt,
            'INFO:btrccts:Stopped because of KeyboardInterrupt: aa')

    def test__main_loop__cancellederror_in_sleep(self):
        self.template__main_loop__exit_exception_during_sleep(
            asyncio.CancelledError,
            'INFO:btrccts:Stopped because of CancelledError: aa')


class AsyncAlgo(AlgorithmBase):

    def __init__(self, mock):
        self._mock = mock

    async def next_iteration(self):
        return self._mock.next_iteration()

    async def exit(self, reason):
        return self._mock.exit(reason=reason)

    async def handle_exception(self, e):
        return self._mock.handle_exception(e)


class AsyncMainLoopTests(MainLoopTests):

    def algo(self, algorithm):
        return AsyncAlgo(algorithm)


class ExecuteAlgorithmTests(unittest.TestCase):

    def run_test(self, Algo):
        with self.assertLogs('btrccts'):
            result = execute_algorithm(exchange_names=['kraken', 'okex'],
                                       symbols=[],
                                       live=False,
                                       auth_aliases={},
                                       AlgorithmClass=Algo,
                                       args=self,
                                       start_balances={'okex': {'ETH': 3},
                                                       'kraken': {'USD': 100}},
                                       pd_start_date=pd_ts('2019-10-01 10:10'),
                                       pd_end_date=pd_ts('2019-10-01 10:16'),
                                       pd_interval=pandas.Timedelta(minutes=2),
                                       data_dir=data_dir)
        return result

    @patch('ccxt.okex.fetch_markets')
    @patch('ccxt.kraken.fetch_markets')
    @patch('ccxt.kraken.fetch_currencies')
    def test__execute_algorithm(self, kraken_currencies,
                                kraken_markets, okex_markets):
        okex_markets.side_effect = fetch_markets_return([ETH_BTC_MARKET])
        kraken_markets.side_effect = fetch_markets_return([BTC_USD_MARKET])
        kraken_currencies.return_value = []
        result = self.run_test(TestAlgo)
        self.assertEqual(result.args, self)
        assert_test_algo_result(self, result, live=False)

    @patch('ccxt.async_support.okex.fetch_markets')
    @patch('ccxt.async_support.kraken.fetch_markets')
    @patch('ccxt.async_support.kraken.fetch_currencies')
    def test__execute_algorithm__async(self, kraken_currencies,
                                       kraken_markets, okex_markets):
        okex_markets.side_effect = async_fetch_markets_return([ETH_BTC_MARKET])
        kraken_markets.side_effect = async_fetch_markets_return(
            [BTC_USD_MARKET])
        kraken_currencies.side_effect = async_return([])
        result = self.run_test(AsyncTestAlgo)
        self.assertEqual(result.args, self)
        assert_test_algo_result(self, result, live=False, async_algo=True)


def execute_algorithm_return_args(**kwargs):
    return kwargs['args']


class ParseParamsAndExecuteAlgorithmTests(unittest.TestCase):

    def create_sys_argv(self, argv_params):
        argv_dict = {'--data-directory': data_dir,
                     '--config-directory': config_dir,
                     '--exchanges': 'kraken',
                     '--symbol': 'BTC/USD',
                     '--start-date': '2001'}
        argv_dict.update(argv_params)
        sys_argv = ['file.py']
        for x, y in argv_dict.items():
            if y is None:
                continue
            sys_argv.append(x)
            if y is not True:
                sys_argv.append(y)
        return sys_argv

    @patch('ccxt.okex.fetch_markets')
    @patch('ccxt.kraken.fetch_markets')
    @patch('ccxt.kraken.fetch_currencies')
    def test__parse_params_and_execute_algorithm(
            self, kraken_currencies, kraken_markets, okex_markets):
        okex_markets.side_effect = fetch_markets_return([ETH_BTC_MARKET])
        kraken_markets.side_effect = fetch_markets_return([BTC_USD_MARKET])
        kraken_currencies.return_value = []
        result = self.run_test(TestAlgo)
        assert_test_algo_result(self, result, live=False)
        self.assertEqual(result.args.algo_bool, True)
        self.assertEqual(result.args.some_string, 'testSTR')
        self.assertEqual(result.args.live, False)

    def run_test(self, Algo):
        sys_argv = self.create_sys_argv({
            '--start-balances': '{"okex": {"ETH": 3},'
                                ' "kraken": {"USD": 100}}',
            '--exchanges': 'kraken,okex',
            '--symbols': '',
            '--start-date': '2019-10-01 10:10',
            '--end-date': '2019-10-01 10:16',
            '--algo-bool': True,
            '--some-string': 'testSTR',
            '--interval': '2m'})
        with patch.object(sys, 'argv', sys_argv):
            with self.assertLogs():
                return parse_params_and_execute_algorithm(Algo)

    @patch('ccxt.async_support.okex.fetch_markets')
    @patch('ccxt.async_support.kraken.fetch_markets')
    @patch('ccxt.async_support.kraken.fetch_currencies')
    def test__parse_params_and_execute_algorithm__async(
            self, kraken_currencies, kraken_markets, okex_markets):
        okex_markets.side_effect = async_fetch_markets_return([ETH_BTC_MARKET])
        kraken_markets.side_effect = async_fetch_markets_return(
            [BTC_USD_MARKET])
        kraken_currencies.side_effect = async_return([])
        result = self.run_test(AsyncTestAlgo)
        assert_test_algo_result(self, result, live=False, async_algo=True)
        self.assertEqual(result.args.algo_bool, True)
        self.assertEqual(result.args.some_string, 'testSTR')
        self.assertEqual(result.args.live, False)

    @patch('btrccts.run.execute_algorithm')
    def template__parse_params_and_execute_algorithm__check_call(
            self, execute_algorithm, argv_params, check_params):
        sys_argv = self.create_sys_argv(argv_params)
        execute_algorithm.side_effect = execute_algorithm_return_args
        with patch.object(sys, 'argv', sys_argv):
            args = parse_params_and_execute_algorithm(TestAlgo)
        params = {
            'AlgorithmClass': TestAlgo,
            'args': args,
            'data_dir': data_dir,
            'conf_dir': config_dir,
            'exchange_names': ['kraken'],
            'pd_end_date': pd_ts('2009-01-01 00:00:00+0000'),
            'pd_start_date': pd_ts('2001-01-01 00:00:00+0000'),
            'pd_interval': pandas.Timedelta('0 days 00:01:00'),
            'start_balances': {},
            'live': False,
            'auth_aliases': {},
            'symbols': ['BTC/USD'],
        }
        params.update(check_params)
        execute_algorithm.assert_called_once_with(**params)

    def template__parse_params_and_execute_algorithm__warning(
            self, logs, argv_params, check_params):
        with self.assertLogs() as cm:
            self.template__parse_params_and_execute_algorithm__check_call(
                argv_params=argv_params, check_params=check_params)
        self.assertEqual(cm.output, logs)

    def test__parse_params_and_execute_algorithm__no_symbols_warning(self):
        self.template__parse_params_and_execute_algorithm__warning(
            argv_params={'--symbols': ''}, check_params={'symbols': []},
            logs=['WARNING:btrccts:No symbols specified, load all ohlcvs '
                  'per each exchange. This can lead to long start times'])

    def test__parse_params_and_execute_algorithm__no_exchanges_warning(self):
        self.template__parse_params_and_execute_algorithm__warning(
            argv_params={'--exchanges': ''},
            check_params={'exchange_names': []},
            logs=['WARNING:btrccts:No exchanges specified, do not load ohlcv'])

    @patch('btrccts.run.execute_algorithm')
    def template__parse_params_and_execute_algorithm__exception(
            self, execute_algorithm, argv_params, exception, exception_test):
        sys_argv = self.create_sys_argv(argv_params)
        with patch.object(sys, 'argv', sys_argv):
            with self.assertRaises(exception) as e:
                parse_params_and_execute_algorithm(TestAlgo)
        self.assertEqual(str(e.exception), exception_test)
        execute_algorithm.assert_not_called()

    def test__parse_params_and_execute_algorithm__start_date_wrong(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={'--start-date': ''}, exception=ValueError,
            exception_test='Start date is not valid')

    def test__parse_params_and_execute_algorithm__end_date_wrong(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={'--end-date': ''}, exception=ValueError,
            exception_test='End date is not valid')

    def test__parse_params_and_execute_algorithm__interval_empty(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={'--interval': ''}, exception=ValueError,
            exception_test='Interval is not valid')

    def test__parse_params_and_execute_algorithm__interval_wrong(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={'--interval': '1X'}, exception=ValueError,
            exception_test='Interval is not valid')

    def test__parse_params_and_execute_algorithm__multiple_exchanges(self):
        self.template__parse_params_and_execute_algorithm__check_call(
            argv_params={'--exchanges': 'kraken,okex,bitfinex'},
            check_params={'exchange_names': ['kraken', 'okex', 'bitfinex']})

    def test__parse_params_and_execute_algorithm__multiple_symbols(self):
        self.template__parse_params_and_execute_algorithm__check_call(
            argv_params={'--symbols': 'BTC/USD,ETH/BTC,XRP/ETH'},
            check_params={'symbols': ['BTC/USD', 'ETH/BTC', 'XRP/ETH']})

    # Live mode tests
    def test__parse_params_and_execute_algorithm__live_start_date(self):
        now_floor = pandas.Timestamp.now(tz='UTC').floor('1T')
        self.template__parse_params_and_execute_algorithm__check_call(
            argv_params={'--live': True, '--start-date': None},
            check_params={'live': True,
                          'pd_start_date': now_floor,
                          'start_balances': None})

    def test__parse_params_and_execute_algorithm__live_auth_aliases(self):
        now_floor = pandas.Timestamp.now(tz='UTC').floor('1T')
        self.template__parse_params_and_execute_algorithm__check_call(
            argv_params={'--live': True,
                         '--start-date': None,
                         '--auth-aliases': '{"binance": "binance_5", '
                                           ' "bitfinex": "b5"}'},
            check_params={'live': True,
                          'pd_start_date': now_floor,
                          'start_balances': None,
                          'auth_aliases': {'binance': 'binance_5',
                                           'bitfinex': 'b5'}})

    def test__parse_params_and_execute_algorithm__live_start_date_set(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={'--live': True, '--start-date': '2001'},
            exception=ValueError,
            exception_test='Start date cannot be set in live mode')

    def test__parse_params_and_execute_algorithm__live_start_balance_set(self):
        self.template__parse_params_and_execute_algorithm__exception(
            argv_params={
                '--live': True, '--start-date': None,
                '--start-balances': '{"okex": {"ETH": 3},'
                                    ' "kraken": {"USD": 100}}'},
            exception=ValueError,
            exception_test='Start balance cannot be set in live mode')


class SleepUntilTests(unittest.TestCase):

    @async_test
    @patch('btrccts.run.asyncio.sleep')
    async def test__sleep_until__none(self, sleep_mock):
        with self.assertRaises(TypeError):
            await sleep_until(None)
        sleep_mock.assert_not_called()

    @patch('btrccts.run.pandas.Timestamp.now')
    @patch('btrccts.run.asyncio.sleep')
    @async_test
    async def test__sleep_until__one_longer_sleep(self, sleep_mock, now_mock):
        dates = pandas.to_datetime(
            ['2017-08-18 00:00:00', '2017-08-18 00:01:00'], utc=True)
        now_mock.side_effect = dates
        sleep_mock.side_effect = async_noop
        await sleep_until(dates[1])
        sleep_mock.assert_called_once_with(1)
        self.assertEqual(now_mock.mock_calls, [call(tz='UTC')] * 2)

    @patch('btrccts.run.pandas.Timestamp.now')
    @patch('btrccts.run.asyncio.sleep')
    @async_test
    async def test__sleep_until__one_partial_sleep(self, sleep_mock, now_mock):
        dates = pandas.to_datetime(
            ['2017-08-18 00:00:00.2', '2017-08-18 00:00:01'], utc=True)
        now_mock.side_effect = dates
        sleep_mock.side_effect = async_noop
        await sleep_until(dates[1])
        sleep_mock.assert_called_once_with(0.8)
        self.assertEqual(now_mock.mock_calls, [call(tz='UTC')] * 2)

    @patch('btrccts.run.pandas.Timestamp.now')
    @patch('btrccts.run.asyncio.sleep')
    @async_test
    async def test__sleep_until__multiple_sleeps(self, sleep_mock, now_mock):
        dates = pandas.to_datetime(
            ['2017-08-18 00:00:00', '2017-08-18 00:00:01.123',
             '2017-08-18 00:00:02.24', '2017-08-18 00:00:03.1'], utc=True)
        now_mock.side_effect = dates
        sleep_mock.side_effect = async_noop
        await sleep_until(pandas.Timestamp('2017-08-18 00:00:03', tz='UTC'))
        self.assertEqual(sleep_mock.mock_calls, [call(1), call(1), call(0.76)])
        self.assertEqual(now_mock.mock_calls, [call(tz='UTC')] * 4)

    @patch('btrccts.run.pandas.Timestamp.now')
    @patch('btrccts.run.asyncio.sleep')
    @async_test
    async def test__sleep_until__clock_changed(self, sleep_mock, now_mock):
        dates = pandas.to_datetime(
            ['2017-08-18 00:01:00', '2017-08-18 00:00:59',
             '2017-08-18 00:01:00', '2017-08-18 00:01:01'], utc=True)
        now_mock.side_effect = dates
        sleep_mock.side_effect = async_noop
        await sleep_until(pandas.Timestamp('2017-08-18 00:01:01', tz='UTC'))
        self.assertEqual(sleep_mock.mock_calls, [call(1)] * 3)
        self.assertEqual(now_mock.mock_calls, [call(tz='UTC')] * 4)
