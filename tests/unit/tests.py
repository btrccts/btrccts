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
    suite = unittest.TestSuite([
        unittest.makeSuite(BacktestContextTest),
        unittest.makeSuite(LiveContextTest),
        unittest.makeSuite(BacktestExchangeBaseTest),
        unittest.makeSuite(AsyncBacktestExchangeBaseTest),
        unittest.makeSuite(BalanceTest),
        unittest.makeSuite(ExchangeAccountTest),
        unittest.makeSuite(ExchangeBackendTest),
        unittest.makeSuite(ExecuteAlgorithmTests),
        unittest.makeSuite(LoadCSVTests),
        unittest.makeSuite(MainLoopTests),
        unittest.makeSuite(AsyncMainLoopTests),
        unittest.makeSuite(ParseParamsAndExecuteAlgorithmTests),
        unittest.makeSuite(Pep8Test),
        unittest.makeSuite(TimeframeTest),
        unittest.makeSuite(SleepUntilTests),
    ])
    return suite
