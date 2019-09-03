import ccxt


class BacktestExchangeBase:
    pass


def create_exchange(exchange_id, params={}):
    if exchange_id not in ccxt.exchanges:
        raise ValueError('Unknown exchange: {}'.format(exchange_id))
    exchange = getattr(ccxt, exchange_id)

    class BacktestExchange(BacktestExchangeBase, exchange):
        pass

    instance = BacktestExchange(params)
    return instance
