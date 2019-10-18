import pandas
import os
import sys
import unittest
from sccts.algorithm import AlgorithmBase
from sccts.run import ExitReason, \
    execute_algorithm, parse_params_and_execute_algorithm
from tests.common import pd_ts
from unittest.mock import patch

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


def assert_test_algo_result(self, result):
    self.assertEqual(type(result), TestAlgo)
    self.assertEqual(result.exit_reason, ExitReason.FINISHED)
    self.assertEqual(result.iterations, 4)
    self.assertEqual(result.okex3.fetch_balance()['total'],
                     {'BTC': 199.7, 'ETH': 1.0})
    self.assertEqual(result.kraken.fetch_balance()['total'],
                     {'BTC': 0.1, 'USD': 99.09865})


class ExecuteAlgorithmIntegrationTests(unittest.TestCase):

    def test__execute_algorithm(self):
        result = execute_algorithm(exchange_names=['kraken', 'okex3'],
                                   symbols=[],
                                   AlgorithmClass=TestAlgo,
                                   args=self,
                                   start_balances={'okex3': {'ETH': 3},
                                                   'kraken': {'USD': 100}},
                                   pd_start_date=pd_ts('2019-10-01 10:10'),
                                   pd_end_date=pd_ts('2019-10-01 10:16'),
                                   pd_timedelta=pandas.Timedelta(minutes=2),
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
            sys_argv.append(x)
            if y is not None:
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
            '--algo-bool': None,
            '--some-string': 'testSTR',
            '--timedelta': '2m'})
        with patch.object(sys, 'argv', sys_argv):
            with self.assertLogs():
                result = parse_params_and_execute_algorithm(TestAlgo)
        assert_test_algo_result(self, result)
        self.assertEqual(result.args.algo_bool, True)
        self.assertEqual(result.args.some_string, 'testSTR')
        self.assertEqual(result.args.live, False)
