import unittest
import re
from unittest.mock import MagicMock
from sccts.backtest import Backtest
from sccts.exchange import check_has
from sccts.backtest import ccxt


ccxt_has = [
    'cancelAllOrders', 'cancelOrder', 'cancelOrders', 'createDepositAddress',
    'createOrder', 'deposit', 'editOrder', 'fetchBalance', 'fetchClosedOrders',
    'fetchCurrencies', 'fetchDepositAddress', 'fetchDeposits',
    'fetchL2OrderBook', 'fetchLedger', 'fetchMarkets', 'fetchMyTrades',
    'fetchOHLCV', 'fetchOpenOrders', 'fetchOrder', 'fetchOrderBook',
    'fetchOrderBooks', 'fetchOrders', 'fetchStatus', 'fetchTicker',
    'fetchTickers', 'fetchTime', 'fetchTrades', 'fetchTradingFee',
    'fetchTradingFees', 'fetchFundingFee', 'fetchFundingFees',
    'fetchTradingLimits', 'fetchTransactions', 'fetchWithdrawals', 'withdraw']
ccxt_has_other = ['CORS', 'createLimitOrder', 'createMarketOrder']
ccxt_has_implemented = ['fetchCurrencies', 'fetchMarkets']


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def camel_case_to_snake_case(name):
    name = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', name).lower()


def patch_exchange_method(exchange_id, method):
    # Please find a working and more elegant way
    # Normal patch is not working, probably due to multiple inheritance
    exchange = getattr(ccxt, exchange_id)
    orig_init = exchange.__init__
    mock = MagicMock()

    def instanciate(self, config):
        res = orig_init(self, config)
        setattr(self, method, mock)
        return res

    exchange.__init__ = instanciate

    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, mock, **kwargs)
        return wrapper
    return decorator


class BacktestExchangeBaseTest(unittest.TestCase):

    def setUp(self):
        self.backtest = Backtest()

    def test__check_has(self):
        exchange = MagicMock()
        exchange.id = 'mock'
        exchange.has = {'has_it': True,
                        'dont_have_it': False,
                        'emu': 'emulated'}
        func = MagicMock()
        # dont has it
        decorator = check_has('dont_have_it')
        wrapped_func = decorator(func)
        with self.assertRaises(NotImplementedError) as e:
            wrapped_func(exchange)
        self.assertEqual(str(e.exception),
                         'mock: method not implemented: dont_have_it')
        func.assert_not_called()
        # has
        decorator = check_has('has_it')
        wrapped_func = decorator(func)
        result = wrapped_func(exchange)
        func.assert_called_once_with(exchange)
        self.assertEqual(result, func())
        # emulated
        func.reset_mock()
        decorator = check_has('emu')
        wrapped_func = decorator(func)
        result = wrapped_func(exchange)
        func.assert_called_once_with(exchange)
        self.assertEqual(result, func())

    def test__exchange_methods_check_has(self):
        exchange = self.backtest.create_exchange('binance')
        for i in ccxt_has:
            exchange.has[i] = False
            snake_case = camel_case_to_snake_case(i)
            with self.assertRaises(NotImplementedError) as e:
                getattr(exchange, snake_case)()
            self.assertEqual(str(e.exception),
                             'binance: method not implemented: {}'.format(i))
            exchange.has[i] = True
            if i not in ccxt_has_implemented:
                with self.assertRaises(NotImplementedError) as e:
                    getattr(exchange, snake_case)()
                self.assertEqual(str(e.exception),
                                 'BacktestExchange does not support method {}'
                                 .format(snake_case))

    @patch_exchange_method('bittrex', 'fetch_markets')
    def test__fetch_markets(self, method):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.fetch_markets({'test': 123})
        method.assert_called_once_with(
            {'test': 123})
        self.assertEqual(result, method())

    @patch_exchange_method('bittrex', 'fetch_currencies')
    def test__fetch_currencies(self, method):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.fetch_currencies({'test': 123})
        method.assert_called_once_with(
            {'test': 123})
        self.assertEqual(result, method())
