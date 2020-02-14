import ccxt
import pandas
import os
import json
import functools
import logging
from collections import defaultdict
from enum import auto, Enum
from btrccts.exchange import BacktestExchangeBase
from btrccts.exchange_backend import ExchangeBackend


class StopException(SystemExit):
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

    def create_exchange(self, exchange_id, config={}):
        if exchange_id not in ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(ccxt, exchange_id)

        class BacktestExchange(BacktestExchangeBase, exchange):
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

    def create_exchange(self, exchange_id, config={}):
        if exchange_id not in ccxt.exchanges:
            raise ValueError('Unknown exchange: {}'.format(exchange_id))
        exchange = getattr(ccxt, exchange_id)
        config_file = os.path.join(self._conf_dir, '{}.json'.format(
            self._auth_aliases.get(exchange_id, exchange_id)))
        loaded_config = {}
        if os.path.isfile(config_file):
            with open(config_file) as f:
                loaded_config = json.load(f)
        else:
            logger = logging.getLogger(__package__)
            logger.warning('Config file for exchange {} does not exist: {}'
                           .format(exchange_id, config_file))
        loaded_config.update(config)
        return exchange(loaded_config)

    def date(self):
        return self._timeframe.date()

    def real_date(self):
        return pandas.Timestamp.now(tz='UTC')

    def state(self):
        return ContextState.LIVE

    def stop(self, msg):
        raise StopException(msg)
