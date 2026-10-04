import unittest
from tests.integration.exchange import BacktestExchangeBaseIntegrationTest
from tests.integration.async_exchange import \
    AsyncBacktestExchangeBaseIntegrationTest
from tests.integration.run import (
    ParseParamsAndExecuteAlgorithmIntegrationTests, MainLoopIntegrationTest,
    ExecuteAlgorithmIntegrationTests)


def test_suite():
    load_tests = unittest.defaultTestLoader.loadTestsFromTestCase
    suite = unittest.TestSuite([
        load_tests(AsyncBacktestExchangeBaseIntegrationTest),
        load_tests(BacktestExchangeBaseIntegrationTest),
        load_tests(ExecuteAlgorithmIntegrationTests),
        load_tests(MainLoopIntegrationTest),
        load_tests(ParseParamsAndExecuteAlgorithmIntegrationTests),
    ])
    return suite
