import unittest
from tests.unit.backtest import BacktestTest, TimeframeTest
from tests.unit.exchange import BacktestExchangeBaseTest
from tests.unit.exchange_backend import ExchangeBackendTest
from tests.unit.exchange_account import ExchangeAccountTest
from tests.unit.balance import BalanceTest
from tests.unit.pep_checker import Pep8Test


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
