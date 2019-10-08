import unittest
from tests.unittests.backtest import BacktestTest, TimeframeTest
from tests.unittests.exchange import BacktestExchangeBaseTest
from tests.unittests.exchange_backend import ExchangeBackendTest
from tests.unittests.exchange_account import ExchangeAccountTest
from tests.unittests.balance import BalanceTest
from tests.unittests.pep_checker import Pep8Test


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(BacktestTest),
        unittest.makeSuite(BacktestExchangeBaseTest),
        unittest.makeSuite(BalanceTest),
        unittest.makeSuite(ExchangeAccountTest),
        unittest.makeSuite(ExchangeBackendTest),
        unittest.makeSuite(Pep8Test),
        unittest.makeSuite(TimeframeTest),
    ])
    return suite