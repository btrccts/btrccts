import unittest
from tests.integration.exchange import BacktestExchangeBaseIntegrationTest
from tests.integration.async_exchange import \
    AsyncBacktestExchangeBaseIntegrationTest
from tests.integration.run import (
    ParseParamsAndExecuteAlgorithmIntegrationTests, MainLoopIntegrationTest,
    ExecuteAlgorithmIntegrationTests)


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(AsyncBacktestExchangeBaseIntegrationTest),
        unittest.makeSuite(BacktestExchangeBaseIntegrationTest),
        unittest.makeSuite(ExecuteAlgorithmIntegrationTests),
        unittest.makeSuite(MainLoopIntegrationTest),
        unittest.makeSuite(ParseParamsAndExecuteAlgorithmIntegrationTests),
    ])
    return suite
