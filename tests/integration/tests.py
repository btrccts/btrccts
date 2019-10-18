import unittest
from tests.integration.exchange import BacktestExchangeBaseIntegrationTest
from tests.integration.run import (
    ParseParamsAndExecuteAlgorithmIntegrationTests,
    ExecuteAlgorithmIntegrationTests)


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(BacktestExchangeBaseIntegrationTest),
        unittest.makeSuite(ExecuteAlgorithmIntegrationTests),
        unittest.makeSuite(ParseParamsAndExecuteAlgorithmIntegrationTests),
    ])
    return suite
