import unittest
from tests.integration.exchange_account import ExchangeAccountIntegrationTest
from tests.integration.run import (
    ParseParamsAndExecuteAlgorithmIntegrationTests,
    ExecuteAlgorithmIntegrationTests)


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(ExecuteAlgorithmIntegrationTests),
        unittest.makeSuite(ExchangeAccountIntegrationTest),
        unittest.makeSuite(ParseParamsAndExecuteAlgorithmIntegrationTests),
    ])
    return suite
