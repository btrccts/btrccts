import unittest
from tests.integration.exchange_account import ExchangeAccountIntegrationTest


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(ExchangeAccountIntegrationTest),
    ])
    return suite
