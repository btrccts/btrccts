import pandas


ETH_BTC_MARKET = {
    'id': 'ETH/BTC',
    'symbol': 'ETH/BTC',
    'base': 'ETH',
    'quote': 'BTC',
    'baseId': 'ETH',
    'quoteId': 'BTC',
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
    'info': {},
    'active': True,
}


def fetch_markets_return(markets):
    result = {m['symbol']: m for m in markets}
    return lambda *a, **b: result


def pd_ts(s):
    return pandas.Timestamp(s, tz='UTC')
