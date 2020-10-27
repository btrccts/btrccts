import unittest
from btrccts.context import BacktestContext
from tests.integration.exchange import KRAKEN_BTC_USD, KRAKEN_BTC_CURRENCY, \
    KRAKEN_MARKET_BTC_USD
from tests.common import async_test


class AsyncBacktestExchangeBaseIntegrationTest(unittest.TestCase):

    def exchange(self):
        backtest = BacktestContext(timeframe=None, exchange_backends={})
        return backtest.create_exchange('kraken', async_ccxt=True)

    @async_test
    async def test__fetch_markets(self):
        kraken = self.exchange()
        markets_list = await kraken.fetch_markets()
        await kraken.close()
        market = list(filter(lambda x: x['symbol'] == 'BTC/USD', markets_list))
        del market[0]['info']
        self.assertEqual(market, [KRAKEN_BTC_USD])

    @async_test
    async def test__fetch_currencies(self):
        kraken = self.exchange()
        currencies = await kraken.fetch_currencies()
        await kraken.close()
        del currencies['BTC']['info']
        self.assertEqual(currencies['BTC'], KRAKEN_BTC_CURRENCY)

    @async_test
    async def test__load_markets(self):
        kraken = self.exchange()
        await kraken.load_markets()
        await kraken.close()
        del kraken.markets['BTC/USD']['info']
        del kraken.currencies['BTC']['info']
        self.assertEqual(kraken.currencies['BTC'], KRAKEN_BTC_CURRENCY)
        self.assertEqual(kraken.markets['BTC/USD'], KRAKEN_MARKET_BTC_USD)
