import unittest
from tests.backtest import BacktestTest
from tests.exchange import BacktestExchangeBaseTest
from tests.pep_checker import Pep8Test


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(BacktestTest),
        unittest.makeSuite(BacktestExchangeBaseTest),
        unittest.makeSuite(Pep8Test),
    ])
    return suite
