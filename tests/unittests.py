import unittest
from tests.exchange import ExchangeMethodsTest
from tests.pep_checker import Pep8Test


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(ExchangeMethodsTest),
        unittest.makeSuite(Pep8Test),
    ])
    return suite
