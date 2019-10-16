import pandas
import os
import unittest
from sccts.algorithm import AlgorithmBase
from sccts.run import load_ohlcvs, serialize_symbol, main_loop, ExitReason, \
    execute_algorithm
from sccts.timeframe import Timeframe
from tests.unit.common import pd_ts
from unittest.mock import Mock, call

here = os.path.dirname(__file__)
data_dir = os.path.join(here, 'run', 'data_dir')
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

    def test__serialize_symbol(self):
        self.assertEqual(serialize_symbol('BTC/USD'), 'BTC_USD')

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

    def setUp(self):
        self.timeframe = Timeframe(pd_start_date=pd_ts('2017-01-01 1:00'),
                                   pd_end_date=pd_ts('2017-01-01 1:03'),
                                   pd_timedelta=pandas.Timedelta(minutes=1))

    def test__main_loop__successful(self):
        algorithm = Mock(spec=AlgorithmBase)
        error = ValueError('a')
        algorithm.next_iteration.side_effect = [0, 0, error, 0]
        with self.assertLogs('sccts') as cm:
            result = main_loop(timeframe=self.timeframe, algorithm=algorithm)
        self.assertEqual(result, algorithm)
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.next_iteration(),
                          call.handle_exception(error),
                          call.next_iteration(),
                          call.exit(reason=ExitReason.FINISHED)])
        self.assertEqual(len(cm.output), 4)
        self.assertEqual(cm.output[0:2],
                         ['INFO:sccts:Starting main_loop',
                          'ERROR:sccts:Error occured during next_iteration'])
        self.assertTrue(cm.output[2].startswith(
            'ERROR:sccts:a\nTraceback (most recent call last):\n  File'))
        self.assertEqual(cm.output[3], 'INFO:sccts:Finished main_loop')

    def test__main_loop__handle_exception_throws(self):
        algorithm = Mock(spec=AlgorithmBase)
        error = ValueError('a')
        algorithm.next_iteration.side_effect = [0, error, 0, 0]
        algorithm.handle_exception.side_effect = AttributeError('side')
        with self.assertLogs('sccts') as cm:
            with self.assertRaises(AttributeError) as e:
                main_loop(timeframe=self.timeframe, algorithm=algorithm)
        self.assertEqual(str(e.exception), 'side')
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.handle_exception(error),
                          call.exit(reason=ExitReason.EXCEPTION)])
        self.assertEqual(len(cm.output), 5)
        self.assertEqual(cm.output[0:2],
                         ['INFO:sccts:Starting main_loop',
                          'ERROR:sccts:Error occured during next_iteration'])
        self.assertTrue(cm.output[2].startswith(
            'ERROR:sccts:a\nTraceback (most recent call last):\n  File'))
        self.assertEqual(cm.output[3], 'ERROR:sccts:Exiting because of '
                                       'exception in handle_exception')
        self.assertTrue(cm.output[4].startswith(
            'ERROR:sccts:side\nTraceback (most recent call last):\n  File'))

    def template__main_loop__exit_exception(self, exception_class, log_str):
        algorithm = Mock(spec=AlgorithmBase)
        algorithm.next_iteration.side_effect = [0, exception_class('aa'), 0, 0]
        with self.assertLogs('sccts') as cm:
            with self.assertRaises(exception_class) as e:
                main_loop(timeframe=self.timeframe, algorithm=algorithm)
        self.assertEqual(str(e.exception), 'aa')
        self.assertEqual(algorithm.mock_calls,
                         [call.next_iteration(),
                          call.next_iteration(),
                          call.exit(reason=ExitReason.STOPPED)])
        self.assertEqual(cm.output,
                         ['INFO:sccts:Starting main_loop', log_str])

    def test__main_loop__systemexit(self):
        self.template__main_loop__exit_exception(
            SystemExit, 'INFO:sccts:Stopped because of SystemExit: aa')

    def test__main_loop__keyboardinterrupt(self):
        self.template__main_loop__exit_exception(
            KeyboardInterrupt,
            'INFO:sccts:Stopped because of KeyboardInterrupt: aa')


class ExecuteAlgorithmTests(unittest.TestCase):

    def test__execute_algorithm(self):
        # TODO: Patch load_markets for exchanges

        class Algo(AlgorithmBase):

            def __init__(self, context, args):
                self.args = args
                self.exit_reason = None
                self.iterations = 0
                self.kraken = context.create_exchange('kraken')
                self.okex3 = context.create_exchange('okex3')

            def next_iteration(self):
                self.iterations += 1
                if self.iterations == 1:
                    self.okex3.create_order(type='market', side='sell',
                                            symbol='ETH/BTC', amount=2)
                if self.iterations == 4:
                    self.kraken.create_order(type='market', side='buy',
                                             symbol='BTC/USD', amount=0.1)

            def exit(self, reason):
                self.exit_reason = reason

        result = execute_algorithm(exchange_names=['kraken', 'okex3'],
                                   symbols=[],
                                   AlgorithmClass=Algo,
                                   args=self,
                                   start_balances={'okex3': {'ETH': 3},
                                                   'kraken': {'USD': 100}},
                                   pd_start_date=pd_ts('2019-10-01 10:10'),
                                   pd_end_date=pd_ts('2019-10-01 10:16'),
                                   pd_timedelta=pandas.Timedelta(minutes=2),
                                   data_dir=data_dir)
        self.assertEqual(type(result), Algo)
        self.assertEqual(result.exit_reason, ExitReason.FINISHED)
        self.assertEqual(result.args, self)
        self.assertEqual(result.iterations, 4)
        self.assertEqual(result.okex3.fetch_balance()['total'],
                         {'BTC': 199.7, 'ETH': 1.0})
        self.assertEqual(result.kraken.fetch_balance()['total'],
                         {'BTC': 0.1, 'USD': 99.09865})
