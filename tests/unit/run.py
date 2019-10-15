import pandas
import os
import unittest
from sccts.run import load_ohlcvs, serialize_symbol

here = os.path.dirname(__file__)
test_dir = os.path.join(here, 'run', 'load_ohlcvs')

binance_eth_btc = pandas.DataFrame(
    index=pandas.to_datetime(['2017-08-18 00:00:00', '2017-08-18 00:01:00',
                              '2017-08-18 00:02:00', '2017-08-18 00:03:00']),
    data={'open': [4285.08, 4285.08, 4285.08, 4285.08],
          'high': [4285.08, 4285.08, 4285.08, 4287.09],
          'low': [4285.08, 4285.08, 4285.08, 4285.08],
          'close': [4285.08, 4285.08, 4285.08, 4287.09],
          'volume': [0.022196, 0.463420, 0.215048, 0.926866]})
binance_xrp_eth = pandas.DataFrame(
    index=pandas.to_datetime(['2017-08-18 00:43:00', '2017-08-18 00:44:00']),
    data={'open': [4307.72, 4307.72],
          'high': [4307.72, 4307.72],
          'low': [4307.72, 4302.15],
          'close': [4307.72, 4302.15],
          'volume': [0.458212, 0.397427]})
bitmex_eth_btc = pandas.DataFrame(
    index=pandas.to_datetime(['2017-10-03 09:57:00', '2017-10-03 09:58:00',
                              '2017-10-03 09:59:00', '2017-10-03 10:00:00']),
    data={'open': [4260.64, 4260.65, 4271.99, 4271.97],
          'high': [4260.65, 4260.65, 4271.99, 4271.97],
          'low': [4260.64, 4260.65, 4261.05, 4258.67],
          'close': [4260.65, 4260.65, 4261.05, 4258.67],
          'volume': [0.409203, 0.365193, 0.491765, 0.325784]})
bitmex_xrp_eth = pandas.DataFrame(
    index=pandas.to_datetime(['2017-12-31 23:07', '2017-12-31 23:08',
                              '2017-12-31 23:09']),
    data={'open': [13775.0, 13750.1, 13750.1],
          'high': [13775.0, 13766.8, 13766.8],
          'low': [13750.10, 13718.87, 13736.30],
          'close': [13750.10, 13766.80, 13763.85],
          'volume': [3.088494, 6.079897, 3.522711]})


class LoadCSVTests(unittest.TestCase):

    def assert_frame_equal(self, d1, d2):
        pandas.testing.assert_frame_equal(d1.sort_index(axis=1),
                                          d2.sort_index(axis=1))

    def test__serialize_symbol(self):
        self.assertEqual(serialize_symbol('BTC/USD'), 'BTC_USD')

    def test__load_ohlcvs__symbol_file_does_not_exist(self):
        with self.assertRaises(FileNotFoundError) as e:
            load_ohlcvs(basedir=test_dir,
                        exchange_names=['bittrex'],
                        symbols=['ETH/BTC'])
        self.assertEqual(
            str(e.exception),
            'Cannot find symbol (ETH/BTC) file for exchange (bittrex)')

    def test__load_ohlcvs__exchange_does_not_exist(self):
        with self.assertRaises(FileNotFoundError) as e:
            load_ohlcvs(basedir=test_dir,
                        exchange_names=['inexistent'],
                        symbols=['ETH/BTC'])
        self.assertEqual(
            str(e.exception),
            'Cannot find symbol (ETH/BTC) file for exchange (inexistent)')

    def test__load_ohlcvs__defect_file(self):
        with self.assertRaises(ValueError) as e:
            load_ohlcvs(basedir=test_dir,
                        exchange_names=['defect'],
                        symbols=['XRP/ETH'])
        self.assertEqual(
            str(e.exception),
            'Cannot parse symbol (XRP/ETH) file for exchange (defect)')

    def test__load_ohlcvs(self):
        result = load_ohlcvs(basedir=test_dir,
                             exchange_names=['bitmex', 'binance'],
                             symbols=['ETH/BTC', 'XRP/ETH'])
        self.assertEqual(sorted(result.keys()), ['binance', 'bitmex'])
        self.assertEqual(sorted(result['binance'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assertEqual(sorted(result['bitmex'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assert_frame_equal(result['binance']['ETH/BTC'], binance_eth_btc)
        self.assert_frame_equal(result['binance']['XRP/ETH'], binance_xrp_eth)
        self.assert_frame_equal(result['bitmex']['XRP/ETH'], bitmex_xrp_eth)
        self.assert_frame_equal(result['bitmex']['ETH/BTC'], bitmex_eth_btc)

    def test__load_ohlcvs__all_symbols_per_exchange(self):
        result = load_ohlcvs(basedir=test_dir,
                             exchange_names=['bitmex'],
                             symbols=[])
        self.assertEqual(sorted(result.keys()), ['bitmex'])
        self.assertEqual(sorted(result['bitmex'].keys()),
                         ['ETH/BTC', 'XRP/ETH'])
        self.assert_frame_equal(result['bitmex']['XRP/ETH'], bitmex_xrp_eth)
        self.assert_frame_equal(result['bitmex']['ETH/BTC'], bitmex_eth_btc)
