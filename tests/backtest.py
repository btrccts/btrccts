import ccxt
import unittest
from unittest.mock import patch
from sccts.backtest import Backtest
from sccts.exchange import BacktestExchangeBase
from sccts.exchange_backend import ExchangeBackend


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

    @patch('sccts.backtest.BacktestExchangeBase.__init__')
    def test__create_exchange__parameters(self, base_init_mock):
        base_init_mock.return_value = None
        bitfinex_backend = ExchangeBackend()
        binance_backend = ExchangeBackend()
        backtest = Backtest(exchange_backends={'bitfinex': bitfinex_backend,
                                               'binance': binance_backend})
        backtest.create_exchange('bitfinex', {'parameter': 123})
        base_init_mock.assert_called_once_with(
            config={'parameter': 123},
            backtest=backtest,
            exchange_backend=bitfinex_backend)
