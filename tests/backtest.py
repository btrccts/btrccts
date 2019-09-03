import ccxt
import unittest
from sccts.backtest import Backtest
from sccts.exchange import BacktestExchangeBase


class BacktestTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        backtest = Backtest()
        with self.assertRaises(ValueError) as e:
            backtest.create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    def test__create_exchange__bases(self):
        backtest = Backtest()
        exchange = backtest.create_exchange('bitfinex')
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.bitfinex))

    def test__create_exchange__parameters(self):
        backtest = Backtest()
        exchange = backtest.create_exchange('bitfinex', {'apiKey': 123})
        self.assertEqual(exchange.apiKey, 123)
        self.assertEqual(exchange._backtest, backtest)
