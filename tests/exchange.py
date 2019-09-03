import ccxt
import unittest
from sccts.exchange import create_exchange, BacktestExchangeBase


class ExchangeMethodsTest(unittest.TestCase):

    def test__create_exchange__not_an_exchange(self):
        with self.assertRaises(ValueError) as e:
            create_exchange('not_an_exchange')
        self.assertEqual(str(e.exception), 'Unknown exchange: not_an_exchange')

    def test__create_exchange__bases(self):
        exchange = create_exchange('bitfinex')
        self.assertEqual(exchange.__class__.__bases__,
                         (BacktestExchangeBase, ccxt.bitfinex))

    def test__create_exchange__parameters(self):
        exchange = create_exchange('bitfinex', {'apiKey': 123})
        self.assertEqual(exchange.apiKey, 123)
