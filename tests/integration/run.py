import pandas
import time
import os
import sys
import unittest
from btrccts.algorithm import AlgorithmBase
from btrccts.run import ExitReason, sleep_until, \
    execute_algorithm, parse_params_and_execute_algorithm, main_loop
from btrccts.timeframe import Timeframe
from tests.common import pd_ts
from unittest.mock import patch, MagicMock

here = os.path.dirname(__file__)
data_dir = os.path.join(here, 'run', 'data_dir')


class TestAlgo(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        argparser.add_argument('--algo-bool', action='store_true')
        argparser.add_argument('--some-string', default='')

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


def assert_test_algo_result(test, result):
    test.assertEqual(type(result), TestAlgo)
    test.assertEqual(result.exit_reason, ExitReason.FINISHED)
    test.assertEqual(result.iterations, 4)
    test.assertEqual(result.okex3.fetch_balance()['total'],
                     {'BTC': 199.40045, 'ETH': 1.0})
    test.assertEqual(result.kraken.fetch_balance()['total'],
                     {'BTC': 0.09974, 'USD': 99.09865})


class LiveTestAlgo(AlgorithmBase):

    def __init__(self, context, args):
        self.iterations = 0
        self.iteration_dates = []
        self.args = args
        self.kraken = context.create_exchange('kraken', {'test': 1})
        self.bitfinex = context.create_exchange('bitfinex', {'xxx': 'yyy'})

    @staticmethod
    def configure_argparser(argparser):
        argparser.add_argument('--algo-bool', action='store_true')
        argparser.add_argument('--some-string', default='')

    @staticmethod
    def get_test_time_parameters():
        pd_interval = pandas.Timedelta(seconds=2)
        # Sleep until the beginning of the start, to be sure there is
        # no timing issue
        sleep_until(pandas.Timestamp.now(tz='UTC').ceil(pd_interval))
        start = pandas.Timestamp.now(tz='UTC').floor(pd_interval)
        return {
            'pd_start_date': start,
            'pd_end_date': start + 7 * pd_interval,
            'pd_interval': pd_interval,
        }

    def next_iteration(self):
        self.iteration_dates.append(pandas.Timestamp.now(tz='UTC'))
        if self.iterations == 2:
            time.sleep(6.95)
        if self.iterations == 4:
            time.sleep(2.95)
        self.iterations += 1

    def exit(self, reason):
        self.exit_reason = reason


def assert_test_live_algo_result(test, result, time_parameters,
                                 complete=False):
    test.assertEqual(type(result), LiveTestAlgo)
    test.assertEqual(result.exit_reason, ExitReason.FINISHED)
    test.assertEqual(result.iterations, 6)

    round_to = pandas.Timedelta(seconds=0.1)
    iteration_dates_round = [i.round(round_to) for i in result.iteration_dates]
    start = time_parameters['pd_start_date']
    delta = time_parameters['pd_interval']
    test.assertTrue(start < result.iteration_dates[0] < start + delta)
    test.assertEqual(
        iteration_dates_round[1:],
        [start + delta,
         start + 2 * delta,
         start + 5.5 * delta,
         start + 6 * delta,
         start + 7.5 * delta,
         ])
    if complete:
        test.assertEqual(result.args.algo_bool, True)
        test.assertEqual(result.args.some_string, 'testSTR')
        test.assertEqual(result.args.live, True)
        test.assertEqual(result.kraken.apiKey, '254642562462')
        test.assertEqual(result.kraken.secret, '23523afasd')
        test.assertEqual(result.kraken.param, True)
        test.assertEqual(result.bitfinex.apiKey, 'bf2')
        test.assertEqual(result.bitfinex.secret, 'key12345')
        test.assertEqual(result.bitfinex.xxx, 'yyy')
        test.assertEqual(result.kraken.test, 1)


class ExecuteAlgorithmIntegrationTests(unittest.TestCase):

    def test__execute_algorithm(self):
        result = execute_algorithm(exchange_names=['kraken', 'okex3'],
                                   symbols=[],
                                   AlgorithmClass=TestAlgo,
                                   args=self,
                                   live=False,
                                   auth_aliases={},
                                   start_balances={'okex3': {'ETH': 3},
                                                   'kraken': {'USD': 100}},
                                   pd_start_date=pd_ts('2019-10-01 10:10'),
                                   pd_end_date=pd_ts('2019-10-01 10:16'),
                                   pd_interval=pandas.Timedelta(minutes=2),
                                   data_dir=data_dir)
        self.assertEqual(result.args, self)
        assert_test_algo_result(self, result)


def execute_algorithm_return_args(**kwargs):
    return kwargs['args']


class ParseParamsAndExecuteAlgorithmIntegrationTests(unittest.TestCase):

    def create_sys_argv(self, argv_params):
        argv_dict = {'--data-directory': data_dir,
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

    def test__parse_params_and_execute_algorithm(self):
        sys_argv = self.create_sys_argv({
            '--start-balances': '{"okex3": {"ETH": 3},'
                                ' "kraken": {"USD": 100}}',
            '--exchanges': 'kraken,okex3',
            '--symbols': '',
            '--start-date': '2019-10-01 10:10',
            '--end-date': '2019-10-01 10:16',
            '--algo-bool': True,
            '--some-string': 'testSTR',
            '--interval': '2m'})
        with patch.object(sys, 'argv', sys_argv):
            with self.assertLogs():
                result = parse_params_and_execute_algorithm(TestAlgo)
        assert_test_algo_result(self, result)
        self.assertEqual(result.args.algo_bool, True)
        self.assertEqual(result.args.some_string, 'testSTR')
        self.assertEqual(result.args.live, False)

    def test__parse_params_and_execute_algorithm__live(self):
        time_params = LiveTestAlgo.get_test_time_parameters()
        sys_argv = self.create_sys_argv({
            '--start-date': None,
            '--end-date': str(time_params['pd_end_date']),
            '--algo-bool': True,
            '--live': True,
            '--exchanges': 'kraken',
            '--some-string': 'testSTR',
            '--config-directory': 'tests/integration/run/config_dir',
            '--data-directory': '/',
            '--auth-aliases': '{"kraken": "kraken_5"}',
            '--interval': '{}s'.format(
                int(time_params['pd_interval'].total_seconds()))})
        with patch.object(sys, 'argv', sys_argv):
            with self.assertLogs():
                result = parse_params_and_execute_algorithm(LiveTestAlgo)
        assert_test_live_algo_result(self, result, time_params, True)


class MainLoopIntegrationTest(unittest.TestCase):

    def test__main_loop__live(self):
        algo = LiveTestAlgo(MagicMock(), MagicMock())
        time_params = LiveTestAlgo.get_test_time_parameters()
        timeframe = Timeframe(pd_start_date=time_params['pd_start_date'],
                              pd_end_date=time_params['pd_end_date'],
                              pd_interval=time_params['pd_interval'])
        result = main_loop(timeframe=timeframe, algorithm=algo, live=True)

        assert_test_live_algo_result(self, result, time_params)
