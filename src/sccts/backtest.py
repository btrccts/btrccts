import ccxt
from sccts.exchange import BacktestExchangeBase


class Backtest:

    def create_exchange(self, exchange_id, params={}):
        if exchange_id not in ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(ccxt, exchange_id)

        class BacktestExchange(BacktestExchangeBase, exchange):
            pass

        instance = BacktestExchange(params, backtest=self)
        return instance
