import unittest
import re
import pandas
from unittest.mock import MagicMock, patch
from btrccts.context import BacktestContext
from ccxt.base.exchange import Exchange
from ccxt.base.errors import InvalidOrder, BadRequest
from tests.common import BTC_USD_MARKET


ccxt_has = [
    'cancelAllOrders', 'cancelOrder', 'cancelOrders', 'createDepositAddress',
    'createLimitOrder', 'createMarketOrder',
    'createOrder', 'deposit', 'editOrder', 'fetchBalance', 'fetchClosedOrders',
    'fetchCurrencies', 'fetchDepositAddress', 'fetchDeposits',
    'fetchL2OrderBook', 'fetchLedger', 'fetchMarkets', 'fetchMyTrades',
    'fetchOpenOrders', 'fetchOrders', 'fetchOrderBook', 'fetchOrder',
    'fetchOrderBooks', 'fetchStatus', 'fetchTicker', 'fetchOHLCV',
    'fetchTickers', 'fetchTime', 'fetchTrades', 'fetchTradingFee',
    'fetchTradingFees', 'fetchFundingFee', 'fetchFundingFees',
    'fetchTradingLimits', 'fetchTransactions', 'fetchWithdrawals', 'withdraw']
ccxt_has_other = ['CORS', ]
ccxt_has_implemented = ['cancelOrder', 'createLimitOrder', 'createMarketOrder',
                        'createOrder', 'fetchCurrencies', 'fetchMarkets',
                        'fetchOrder', 'fetchOHLCV', 'fetchBalance',
                        'fetchClosedOrders', 'fetchOpenOrders', 'fetchTicker']

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def camel_case_to_snake_case(name):
    name = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', name).lower()


class BacktestExchangeBaseTest(unittest.TestCase):

    def setUp(self):
        self.binance_backend_mock = MagicMock()
        self.backtest = BacktestContext(
            timeframe=None,
            exchange_backends={'binance': self.binance_backend_mock})

    @patch.object(Exchange, 'load_markets')
    def test__exchange_methods_check_has(self, mock):
        params_per_method = {
            'createLimitOrder': ['BTC/USD', 'sell', 5],
            'createMarketOrder': ['BTC/USD', 'sell', 5],
            'createOrder': ['BTC/USD', 'sell', 'market', 5],
            'fetchOHLCV': ['BTC/USD', '1m'],
            'cancelOrder': ['id'],
            'fetchOrder': ['id'],
            'fetchTicker': ['BTC/USD'],
        }
        exchange = self.backtest.create_exchange('binance')
        for i in ccxt_has:
            exchange.has[i] = False
            snake_case = camel_case_to_snake_case(i)
            params = params_per_method.get(i, [])
            with self.assertRaises(NotImplementedError) as e:
                getattr(exchange, snake_case)(*params)
            self.assertEqual(str(e.exception),
                             'binance: method not implemented: {}'.format(i))
            for state in [True, 'emulated']:
                exchange.has[i] = state
                if i not in ccxt_has_implemented:
                    with self.assertRaises(NotImplementedError) as e:
                        getattr(exchange, snake_case)(*params)
                    self.assertEqual(
                        str(e.exception),
                        'BacktestExchange does not support method {}'
                        .format(snake_case))

    def template__propagate_method_call(self, function_name, parameters):
        exchange = self.backtest.create_exchange('binance')
        result = getattr(exchange, function_name)(**parameters)
        backend_method = getattr(self.binance_backend_mock, function_name)
        backend_method.assert_called_once_with(**parameters)
        self.assertEqual(result, backend_method())

    def test__cancel_order(self):
        self.template__propagate_method_call(
            'cancel_order', {'id': 'some_id', 'symbol': 'BTC/USD'})

    def test__fetch_order(self):
        self.template__propagate_method_call(
            'fetch_order', {'id': 'some_id', 'symbol': 'BTC/USD'})

    def test__fetch_closed_orders(self):
        self.template__propagate_method_call(
            'fetch_closed_orders',
            {'symbol': 'BTC/USD', 'since': 5123, 'limit': 80})

    def test__fetch_open_orders(self):
        self.template__propagate_method_call(
            'fetch_open_orders',
            {'symbol': 'ETH/BTC', 'since': 523, 'limit': 8})

    def test__fetch_balance(self):
        self.binance_backend_mock.fetch_balance.return_value = {
            'BTC': {'free': 15.3, 'total': 15.3, 'used': 0.0},
            'USD': {'free': 0.3, 'total': 0.3, 'used': 0.0}}
        exchange = self.backtest.create_exchange('binance')
        result = exchange.fetch_balance(params={})
        self.binance_backend_mock.fetch_balance.assert_called_once_with()
        self.assertEqual(result,
                         {'BTC': {'free': 15.3, 'total': 15.3, 'used': 0.0},
                          'USD': {'free': 0.3, 'total': 0.3, 'used': 0.0},
                          'free': {'BTC': 15.3, 'USD': 0.3},
                          'total': {'BTC': 15.3, 'USD': 0.3},
                          'used': {'BTC': 0.0, 'USD': 0.0}})

    @patch('ccxt.bittrex.fetch_markets')
    def test__fetch_markets(self, method):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.fetch_markets({'test': 123})
        method.assert_called_once_with(
            {'test': 123})
        self.assertEqual(result, method())

    @patch('ccxt.bittrex.fetch_currencies')
    def test__fetch_currencies(self, fetch_currencies):
        exchange = self.backtest.create_exchange('bittrex')
        result = exchange.fetch_currencies({'test': 123})
        fetch_currencies.assert_called_once_with(
            {'test': 123})
        self.assertEqual(result, fetch_currencies())

    @patch('ccxt.binance.fetch_markets')
    def test__create_order__market_sell(self, fetch_markets_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_markets_mock.return_value = [BTC_USD_MARKET]
        result = exchange.create_order(symbol='BTC/USD', type='market',
                                       side='sell', amount=5)
        fetch_markets_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_called_once_with(
            amount=5, price=None, side='sell', type='market',
            market=exchange.markets['BTC/USD'])
        self.assertEqual(result, self.binance_backend_mock.create_order())

    @patch('ccxt.binance.fetch_markets')
    def test__create_order__limit_buy(self, fetch_markets_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_markets_mock.return_value = [BTC_USD_MARKET]
        result = exchange.create_order(symbol='BTC/USD', type='limit',
                                       side='buy', amount=2, price=17)
        fetch_markets_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_called_once_with(
            amount=2, price=17, side='buy', type='limit',
            market=exchange.markets['BTC/USD'])
        self.assertEqual(result, self.binance_backend_mock.create_order())

    @patch('ccxt.binance.fetch_markets')
    def test__create_order__no_market(self, fetch_markets_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_markets_mock.return_value = [BTC_USD_MARKET]
        with self.assertRaises(InvalidOrder) as e:
            exchange.create_order(symbol='ETH/USD', type='market', side='sell',
                                  amount=5)
        self.assertEqual(str(e.exception),
                         'Exchange: market does not exist: ETH/USD')
        fetch_markets_mock.assert_called_once_with({})
        self.binance_backend_mock.create_order.assert_not_called()

    @patch('ccxt.binance.fetch_markets')
    def test__create_order__fetch_markets_only_once(self, fetch_markets_mock):
        exchange = self.backtest.create_exchange('binance')
        fetch_markets_mock.return_value = [BTC_USD_MARKET]
        exchange.create_order(symbol='BTC/USD', type='market', side='sell',
                              amount=5)
        exchange.create_order(symbol='BTC/USD', type='market', side='sell',
                              amount=5)
        fetch_markets_mock.assert_called_once_with({})

    def test__fetch_ohlcv(self):
        df = pandas.DataFrame(data={'open': [2, 4],
                                    'high': [3, 5],
                                    'low': [1, 3],
                                    'close': [4, 6],
                                    'volume': [102, 110]},
                              index=pandas.to_datetime(['2017-01-01 1:00',
                                                        '2017-01-01 1:01'],
                                                       utc=True))
        fetch_ohlcv_dataframe_mock = \
            self.binance_backend_mock.fetch_ohlcv_dataframe
        fetch_ohlcv_dataframe_mock.return_value = df
        exchange = self.backtest.create_exchange('binance')
        result = exchange.fetch_ohlcv('BTC/USD', '5m', limit=2, since=50)
        self.assertEqual(result, [[1483232400000, 2, 3, 1, 4, 102],
                                  [1483232460000, 4, 5, 3, 6, 110]])
        fetch_ohlcv_dataframe_mock.assert_called_once_with(
            symbol='BTC/USD', timeframe='5m', limit=2, since=50)

    def test__fetch_ohlcv__timeframe_not_in_timeframes(self):
        exchange = self.backtest.create_exchange('poloniex')
        with self.assertRaises(BadRequest) as e:
            exchange.fetch_ohlcv('BTC/USD', '1m')
        self.assertEqual(str(e.exception),
                         'Timeframe 1m not supported by exchange')
