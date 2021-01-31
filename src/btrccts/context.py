import ccxt
import ccxt.async_support
import pandas
import os
import json
import functools
import logging
from collections import defaultdict
from enum import auto, Enum
from btrccts.exchange import BacktestExchangeBase
from btrccts.async_exchange import AsyncBacktestExchangeBase
from btrccts.exchange_backend import ExchangeBackend
try:
    import ccxtpro
except ImportError:
    ccxtpro = None
    pass


class StopException(BaseException):
    pass


class ContextState(Enum):

    BACKTEST = auto()
    LIVE = auto()


class BacktestContext:

    def __init__(self, timeframe, exchange_backends={}):
        self._exchange_backends = defaultdict(functools.partial(
            ExchangeBackend, timeframe=timeframe))
        for key in exchange_backends:
            self._exchange_backends[key] = exchange_backends[key]
        self._timeframe = timeframe

    def create_exchange(self, exchange_id, config={}, async_ccxt=False):
        use_ccxt = ccxt
        base = BacktestExchangeBase
        if async_ccxt:
            use_ccxt = ccxt.async_support
            base = AsyncBacktestExchangeBase
        if exchange_id not in use_ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(use_ccxt, exchange_id)

        class BacktestExchange(base, exchange):
            pass

        backend = self._exchange_backends[exchange_id]
        instance = BacktestExchange(config=config, exchange_backend=backend)
        return instance

    def date(self):
        return self._timeframe.date()

    def real_date(self):
        return self._timeframe.date()

    def state(self):
        return ContextState.BACKTEST

    def stop(self, msg):
        raise StopException(msg)


class LiveContext:

    def __init__(self, timeframe, conf_dir, auth_aliases={}):
        self._timeframe = timeframe
        self._auth_aliases = auth_aliases
        self._conf_dir = conf_dir

    def create_exchange(self, exchange_id, config={}, async_ccxt=False):
        use_ccxt = ccxt
        if async_ccxt:
            use_ccxt = ccxt.async_support
            if ccxtpro is not None:
                if hasattr(ccxtpro, exchange_id):
                    use_ccxt = ccxtpro
        if exchange_id not in use_ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(use_ccxt, exchange_id)
        config_file = os.path.join(self._conf_dir, '{}.json'.format(
            self._auth_aliases.get(exchange_id, exchange_id)))
        exchange_config = {'enableRateLimit': True}
        if os.path.isfile(config_file):
            with open(config_file) as f:
                exchange_config.update(json.load(f))
        else:
            logger = logging.getLogger(__package__)
            logger.warning('Config file for exchange {} does not exist: {}'
                           .format(exchange_id, config_file))
        exchange_config.update(config)
        return exchange(exchange_config)

    def date(self):
        return self._timeframe.date()

    def real_date(self):
        return pandas.Timestamp.now(tz='UTC')

    def state(self):
        return ContextState.LIVE

    def stop(self, msg):
        raise StopException(msg)
