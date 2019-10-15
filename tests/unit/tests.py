import unittest
from tests.unit.backtest import BacktestContextTest
from tests.unit.balance import BalanceTest
from tests.unit.exchange import BacktestExchangeBaseTest
from tests.unit.exchange_account import ExchangeAccountTest
from tests.unit.exchange_backend import ExchangeBackendTest
from tests.unit.pep_checker import Pep8Test
from tests.unit.run import LoadCSVTests, MainLoopTests
from tests.unit.timeframe import TimeframeTest


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(BacktestContextTest),
        unittest.makeSuite(BacktestExchangeBaseTest),
        unittest.makeSuite(BalanceTest),
        unittest.makeSuite(ExchangeAccountTest),
        unittest.makeSuite(ExchangeBackendTest),
        unittest.makeSuite(LoadCSVTests),
        unittest.makeSuite(MainLoopTests),
        unittest.makeSuite(Pep8Test),
        unittest.makeSuite(TimeframeTest),
    ])
    return suite
