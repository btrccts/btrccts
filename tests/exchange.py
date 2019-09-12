import unittest
import re
from unittest.mock import MagicMock, patch
from sccts.backtest import Backtest
from sccts.exchange import check_has
from sccts.backtest import ccxt
from ccxt.base.exchange import Exchange
from ccxt.base.errors import InvalidOrder


ccxt_has = [
    'cancelAllOrders', 'cancelOrder', 'cancelOrders', 'createDepositAddress',
    'createLimitOrder', 'createMarketOrder',
    'createOrder', 'deposit', 'editOrder', 'fetchBalance', 'fetchClosedOrders',
    'fetchCurrencies', 'fetchDepositAddress', 'fetchDeposits',
    'fetchL2OrderBook', 'fetchLedger', 'fetchMarkets', 'fetchMyTrades',
    'fetchOHLCV', 'fetchOpenOrders', 'fetchOrders', 'fetchOrderBook',
    'fetchOrderBooks', 'fetchStatus', 'fetchTicker',
    'fetchTickers', 'fetchTime', 'fetchTrades', 'fetchTradingFee',
    'fetchTradingFees', 'fetchFundingFee', 'fetchFundingFees',
    'fetchTradingLimits', 'fetchTransactions', 'fetchWithdrawals', 'withdraw']
ccxt_has_other = ['CORS', ]
ccxt_has_implemented = ['cancelOrder', 'createLimitOrder', 'createMarketOrder',
                        'createOrder', 'fetchCurrencies', 'fetchMarkets',
                        'fetchOrder']

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

    def decorator(func):
        mock = MagicMock()

        def instanciate(self, config):
            res = orig_init(self, config=config)
            setattr(self, method, mock)
            return res

        def wrapper(*args, **kwargs):
            exchange.__init__ = instanciate
            try:
                result = func(*args, mock, **kwargs)
            except BaseException:
                exchange.__init__ = orig_init
                raise
            exchange.__init__ = orig_init
            return result
        return wrapper
    return decorator


class BacktestExchangeBaseTest(unittest.TestCase):

    def setUp(self):
        self.binance_backend_mock = MagicMock()
        self.backtest = Backtest(
            timeframe=None,
            exchange_backends={'binance': self.binance_backend_mock})
        self.btc_usd_market = {
            'id': 'BTC/USD',
            'symbol': 'BTC/USD',
            'base': 'BTC',
            'quote': 'USD',
            'baseId': 'BTC',
            'quoteId': 'USD',
            'info': {},
            'active': True,
        }

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

    @patch.object(Exchange, 'load_markets')
    def test__exchange_methods_check_has(self, mock):
        exchange = self.backtest.create_exchange('binance')
        for i in ccxt_has:
            exchange.has[i] = False
            snake_case = camel_case_to_snake_case(i)
            params = {}
            if i == 'createLimitOrder':
                params = ['BTC/USD', 'sell', 5]
            elif i == 'createMarketOrder':
                params = ['BTC/USD', 'sell', 5]
            elif i == 'createOrder':
                params = {'symbol': 'BTC/USD',
                          'side': 'sell',
                          'type': 'market',
                          'amount': 5}
            with self.assertRaises(NotImplementedError) as e:
                getattr(exchange, snake_case)(*params)
            self.assertEqual(str(e.exception),
                             'binance: method not implemented: {}'.format(i))
            exchange.has[i] = True
            if i not in ccxt_has_implemented:
                with self.assertRaises(NotImplementedError) as e:
                    getattr(exchange, snake_case)(*params)
                self.assertEqual(str(e.exception),
                                 'BacktestExchange does not support method {}'
                                 .format(snake_case))

    @patch_exchange_method('bittrex', 'cancel_order')
    def test__cancel_order(self, method):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.cancel_order(id='some_id', symbol='BTC/USD')
        method.assert_called_once_with(id='some_id', symbol='BTC/USD')
        self.assertEqual(result, method())

    @patch_exchange_method('bittrex', 'fetch_order')
    def test__fetch_order(self, method):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.fetch_order(id='some_id', symbol='BTC/USD')
        method.assert_called_once_with(id='some_id', symbol='BTC/USD')
        self.assertEqual(result, method())

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

    @patch_exchange_method('binance', 'fetch_markets')
    def test__create_order__market_sell(self, fetch_currencies_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_currencies_mock.return_value = [self.btc_usd_market]
        result = exchange.create_order(symbol='BTC/USD', type='market',
                                       side='sell', amount=5)
        fetch_currencies_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_called_once_with(
            amount=5, price=None, side='sell', type='market',
            market=exchange.markets['BTC/USD'])
        self.assertEqual(result, self.binance_backend_mock.create_order())

    @patch_exchange_method('binance', 'fetch_markets')
    def test__create_order__limit_buy(self, fetch_currencies_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_currencies_mock.return_value = [self.btc_usd_market]
        result = exchange.create_order(symbol='BTC/USD', type='limit',
                                       side='buy', amount=2, price=17)
        fetch_currencies_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_called_once_with(
            amount=2, price=17, side='buy', type='limit',
            market=exchange.markets['BTC/USD'])
        self.assertEqual(result, self.binance_backend_mock.create_order())

    @patch_exchange_method('binance', 'fetch_markets')
    def test__create_order__no_market(self, fetch_currencies_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_currencies_mock.return_value = [self.btc_usd_market]
        with self.assertRaises(InvalidOrder) as e:
            exchange.create_order(symbol='ETH/USD', type='market', side='sell',
                                  amount=5)
        self.assertEqual(str(e.exception),
                         'Exchange: market does not exist: ETH/USD')
        fetch_currencies_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_not_called()

    @patch_exchange_method('binance', 'fetch_markets')
    def test__create_order__fetch_markets_only_once(
            self, fetch_currencies_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_currencies_mock.return_value = [self.btc_usd_market]
        exchange.create_order(symbol='BTC/USD', type='market', side='sell',
                              amount=5)
        exchange.create_order(symbol='BTC/USD', type='market', side='sell',
                              amount=5)
        fetch_currencies_mock.assert_called_once_with({})
