import ccxt
from collections import defaultdict
from sccts.exchange import BacktestExchangeBase
from sccts.exchange_backend import ExchangeBackend


class Backtest:

    def __init__(self, exchange_backends={}):
        self._exchange_backends = defaultdict(ExchangeBackend)
        for key in exchange_backends:
            self._exchange_backends[key] = exchange_backends[key]

    def create_exchange(self, exchange_id, config={}):
        if exchange_id not in ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(ccxt, exchange_id)

        class BacktestExchange(BacktestExchangeBase, exchange):
            pass

        backend = self._exchange_backends[exchange_id]
        instance = BacktestExchange(config=config, exchange_backend=backend,
                                    backtest=self)
        return instance
