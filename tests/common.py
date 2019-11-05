import pandas


ETH_BTC_MARKET = {
    'id': 'ETH/BTC',
    'symbol': 'ETH/BTC',
    'base': 'ETH',
    'quote': 'BTC',
    'baseId': 'ETH',
    'quoteId': 'BTC',
    'maker': 0.005,
    'taker': 0.01,
    'info': {},
    'active': True,
}
BTC_USD_MARKET = {
    'id': 'BTC/USD',
    'symbol': 'BTC/USD',
    'base': 'BTC',
    'quote': 'USD',
    'baseId': 'BTC',
    'quoteId': 'USD',
    'maker': 0.001,
    'taker': 0.002,
    'info': {},
    'active': True,
}


def fetch_markets_return(markets):
    result = {m['symbol']: m for m in markets}
    return lambda *a, **b: result


def pd_ts(s):
    return pandas.Timestamp(s, tz='UTC')
