import pandas
from btrccts.run import _run_async


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


def async_return(result):
    async def func(*args, **kwargs):
        return result
    return func


def async_fetch_markets_return(markets):
    return async_return(fetch_markets_return(markets)())


def pd_ts(s):
    return pandas.Timestamp(s, tz='UTC')


def async_test(coro):
    def wrapper(*args, **kwargs):
        async def func():
            return await coro(*args, **kwargs)
        return _run_async(func())
    return wrapper


async def async_noop(t=None):
    pass
