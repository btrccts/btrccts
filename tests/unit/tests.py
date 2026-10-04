import unittest
from tests.unit.context import BacktestContextTest, LiveContextTest
from tests.unit.balance import BalanceTest
from tests.unit.exchange import BacktestExchangeBaseTest
from tests.unit.async_exchange import AsyncBacktestExchangeBaseTest
from tests.unit.exchange_account import ExchangeAccountTest
from tests.unit.exchange_backend import ExchangeBackendTest
from tests.unit.pep_checker import Pep8Test
from tests.unit.run import LoadCSVTests, MainLoopTests, \
    ExecuteAlgorithmTests, ParseParamsAndExecuteAlgorithmTests, \
    SleepUntilTests, AsyncMainLoopTests
from tests.unit.timeframe import TimeframeTest


def test_suite():
    load_tests = unittest.defaultTestLoader.loadTestsFromTestCase
    suite = unittest.TestSuite([
        load_tests(BacktestContextTest),
        load_tests(LiveContextTest),
        load_tests(BacktestExchangeBaseTest),
        load_tests(AsyncBacktestExchangeBaseTest),
        load_tests(BalanceTest),
        load_tests(ExchangeAccountTest),
        load_tests(ExchangeBackendTest),
        load_tests(ExecuteAlgorithmTests),
        load_tests(LoadCSVTests),
        load_tests(MainLoopTests),
        load_tests(AsyncMainLoopTests),
        load_tests(ParseParamsAndExecuteAlgorithmTests),
        load_tests(Pep8Test),
        load_tests(TimeframeTest),
        load_tests(SleepUntilTests),
    ])
    return suite
