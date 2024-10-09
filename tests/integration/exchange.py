import unittest
from btrccts.context import BacktestContext


KRAKEN_BTC_USD = {
    'active': True,
    'altname': 'XBTUSD',
    'base': 'BTC',
    'baseId': 'XXBT',
    'contract': False,
    'contractSize': None,
    'created': None,
    'darkpool': False,
    'expiry': None,
    'expiryDatetime': None,
    'future': False,
    'id': 'XXBTZUSD',
    'inverse': None,
    'limits': {'amount': {'max': None, 'min': 0.0001},
               'cost': {'max': None, 'min': 0.5},
               'leverage': {'max': 5.0, 'min': 1.0},
               'price': {'max': None, 'min': 0.1}},
    'linear': None,
    'maker': 0.0025,
    'margin': True,
    'option': False,
    'optionType': None,
    'precision': {'amount': 1e-08, 'price': 0.1},
    'quote': 'USD',
    'quoteId': 'ZUSD',
    'settle': None,
    'settleId': None,
    'spot': True,
    'symbol': 'BTC/USD',
    'strike': None,
    'swap': False,
    'taker': 0.004,
    'type': 'spot',
    'wsId': 'XBT/USD'}

KRAKEN_BTC_CURRENCY = {
    'active': True,
    'code': 'BTC',
    'deposit': None,
    'fee': None,
    'id': 'XXBT',
    'limits': {'amount': {'max': None, 'min': 1e-10},
               'withdraw': {'max': None, 'min': None}},
    'name': 'XBT',
    'networks': {},
    'precision': 1e-10,
    'withdraw': None}

KRAKEN_MARKET_BTC_USD = KRAKEN_BTC_USD.copy()
KRAKEN_MARKET_BTC_USD.update({
    'index': None,
    'lowercaseId': None,
    'marginModes': {'cross': None, 'isolated': None},
    'percentage': True,
    'subType': None,
    'tierBased': True,
    'precision': {'amount': 1e-08,
                  'base': None,
                  'cost': None,
                  'price': 0.1,
                  'quote': None},
    'tiers': {'maker': [[0, 0.0016],
                        [50000, 0.0014],
                        [100000, 0.0012],
                        [250000, 0.001],
                        [500000, 0.0008],
                        [1000000, 0.0006],
                        [2500000, 0.0004],
                        [5000000, 0.0002],
                        [10000000, 0.0]],
              'taker': [[0, 0.0026],
                        [50000, 0.0024],
                        [100000, 0.0022],
                        [250000, 0.002],
                        [500000, 0.0018],
                        [1000000, 0.0016],
                        [2500000, 0.0014],
                        [5000000, 0.0012],
                        [10000000, 0.0001]]},
})


class BacktestExchangeBaseIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.backtest = BacktestContext(timeframe=None, exchange_backends={})

    def test__fetch_markets(self):
        kraken = self.backtest.create_exchange('kraken')
        markets_list = kraken.fetch_markets()
        market = list(filter(lambda x: x['symbol'] == 'BTC/USD', markets_list))
        del market[0]['info']
        self.assertEqual(market, [KRAKEN_BTC_USD])

    def test__fetch_currencies(self):
        kraken = self.backtest.create_exchange('kraken')
        currencies = kraken.fetch_currencies()
        del currencies['BTC']['info']
        self.assertEqual(currencies['BTC'], KRAKEN_BTC_CURRENCY)

    def test__load_markets(self):
        kraken = self.backtest.create_exchange('kraken')
        kraken.load_markets()
        del kraken.markets['BTC/USD']['info']
        del kraken.currencies['BTC']['info']
        self.assertEqual(kraken.currencies['BTC'], KRAKEN_BTC_CURRENCY)
        self.assertEqual(kraken.markets['BTC/USD'], KRAKEN_MARKET_BTC_USD)
