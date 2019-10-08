import pandas
import unittest
from sccts.backtest import Timeframe
from sccts.exchange_account import ExchangeAccount


class ExchangeAccountIntegrationTest(unittest.TestCase):

    def test__create_order__multiple_limit_orders(self):
        eth_btc_market = {'base': 'ETH', 'quote': 'BTC',
                          'symbol': 'ETH/BTC'}
        btc_usd_market = {'base': 'BTC', 'quote': 'USD',
                          'symbol': 'BTC/USD'}
        dates = pandas.to_datetime(['2017-06-01 1:00', '2017-06-01 1:01',
                                    '2017-06-01 1:02', '2017-06-01 1:03'],
                                   utc=True)
        timeframe = Timeframe(pd_start_date=dates[0],
                              pd_end_date=dates[-1],
                              pd_timedelta=pandas.Timedelta(minutes=1))
        eth_btc_data = {'high': [10, 9, 11, 9],
                        'low': [9, 8, 7, 6]}
        eth_btc_ohlcvs = pandas.DataFrame(data=eth_btc_data, index=dates)
        btc_usd_data = {'high': [5, 6, 7, 8],
                        'low': [4, 5, 6, 7]}
        btc_usd_ohlcvs = pandas.DataFrame(data=btc_usd_data, index=dates)
        account = ExchangeAccount(timeframe=timeframe,
                                  ohlcvs={'ETH/BTC': eth_btc_ohlcvs,
                                          'BTC/USD': btc_usd_ohlcvs},
                                  balances={'BTC': 50,
                                            'ETH': 100})
        # Fill multiple orders at the same time
        create_result = account.create_order(market=eth_btc_market,
                                             side='buy', type='limit',
                                             amount=1, price=7.5)
        same_time_buy_id = create_result['id']
        create_result = account.create_order(market=btc_usd_market,
                                             side='sell', type='limit',
                                             amount=2, price=6.5)
        same_time_sell_id = create_result['id']
        # Fill on earlier date then order created before
        create_result = account.create_order(market=eth_btc_market,
                                             side='buy', type='limit',
                                             amount=3, price=8.5)
        first_buy_id = create_result['id']
        # Fill at last
        create_result = account.create_order(market=eth_btc_market,
                                             side='buy', type='limit',
                                             amount=2, price=6.1)
        last_buy_id = create_result['id']

        def check_order_open(id):
            order = account.fetch_order(id)
            self.assertEqual(order['status'], 'open')
            self.assertEqual(order['lastTradeTimestamp'], None)
            self.assertEqual(order['filled'], 0)

        def check_order_filled_now(id):
            order = account.fetch_order(id)
            self.assertEqual(order['status'], 'closed')
            self.assertEqual(order['lastTradeTimestamp'],
                             timeframe.date().value / 1e6)
            self.assertTrue(order['filled'] != 0)

        check_order_open(same_time_buy_id)
        check_order_open(same_time_sell_id)
        check_order_open(first_buy_id)
        check_order_open(last_buy_id)

        timeframe.add_timedelta()
        check_order_filled_now(first_buy_id)
        check_order_open(same_time_buy_id)
        check_order_open(same_time_sell_id)
        check_order_open(last_buy_id)

        timeframe.add_timedelta()
        check_order_filled_now(same_time_buy_id)
        check_order_filled_now(same_time_sell_id)
        check_order_open(last_buy_id)

        timeframe.add_timedelta()
        check_order_filled_now(last_buy_id)

        self.assertEqual(
            account.fetch_order(same_time_buy_id),
            {'amount': 1.0,
             'average': 7.5,
             'cost': 7.5,
             'datetime': '2017-06-01T01:00:00.000Z',
             'fee': 0,
             'filled': 1.0,
             'id': '1',
             'info': {},
             'lastTradeTimestamp': 1496278920000,
             'price': 7.5,
             'remaining': 0,
             'side': 'buy',
             'status': 'closed',
             'symbol': 'ETH/BTC',
             'timestamp': 1496278800000,
             'trades': None,
             'type': 'limit'})
        self.assertEqual(
            account.fetch_order(same_time_sell_id),
            {'amount': 2.0,
             'average': 6.5,
             'cost': 13.0,
             'datetime': '2017-06-01T01:00:00.000Z',
             'fee': 0,
             'filled': 2.0,
             'id': '2',
             'info': {},
             'lastTradeTimestamp': 1496278920000,
             'price': 6.5,
             'remaining': 0,
             'side': 'sell',
             'status': 'closed',
             'symbol': 'BTC/USD',
             'timestamp': 1496278800000,
             'trades': None,
             'type': 'limit'})
        self.assertEqual(
            account.fetch_order(first_buy_id),
            {'amount': 3.0,
             'average': 8.5,
             'cost': 25.5,
             'datetime': '2017-06-01T01:00:00.000Z',
             'fee': 0,
             'filled': 3.0,
             'id': '3',
             'info': {},
             'lastTradeTimestamp': 1496278860000,
             'price': 8.5,
             'remaining': 0,
             'side': 'buy',
             'status': 'closed',
             'symbol': 'ETH/BTC',
             'timestamp': 1496278800000,
             'trades': None,
             'type': 'limit'})
        self.assertEqual(
            account.fetch_order(last_buy_id),
            {'amount': 2.0,
             'average': 6.1,
             'cost': 12.2,
             'datetime': '2017-06-01T01:00:00.000Z',
             'fee': 0,
             'filled': 2.0,
             'id': '4',
             'info': {},
             'lastTradeTimestamp': 1496278980000,
             'price': 6.1,
             'remaining': 0,
             'side': 'buy',
             'status': 'closed',
             'symbol': 'ETH/BTC',
             'timestamp': 1496278800000,
             'trades': None,
             'type': 'limit'})
        self.assertEqual(account.fetch_balance(),
                         {'BTC': {'free': 2.8, 'total': 2.8, 'used': 0.0},
                          'ETH': {'free': 106.0, 'total': 106.0, 'used': 0.0},
                          'USD': {'free': 13.0, 'total': 13.0, 'used': 0.0}})
