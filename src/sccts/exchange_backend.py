import pandas
from ccxt.base.exchange import Exchange
from ccxt.base.errors import BadRequest, BadSymbol
from btrccts.check_dataframe import _check_dataframe
from btrccts.exchange_account import ExchangeAccount


class ExchangeBackend:

    def __init__(self, timeframe, balances={}, ohlcvs={}):
        self._account = ExchangeAccount(timeframe=timeframe,
                                        balances=balances,
                                        ohlcvs=ohlcvs)
        self._ohlcvs = {}
        self._timeframe = timeframe
        for key in ohlcvs:
            self._ohlcvs[key] = _check_dataframe(
                ohlcvs[key],
                timeframe,
                ['open', 'low', 'high', 'close', 'volume'])

    def fetch_order(self, id, symbol=None):
        return self._account.fetch_order(id=id, symbol=symbol)

    def fetch_balance(self):
        return self._account.fetch_balance()

    def create_order(self, market, type, price, side, amount):
        return self._account.create_order(market=market, type=type, side=side,
                                          price=price, amount=amount)

    def cancel_order(self, id, symbol=None):
        return self._account.cancel_order(id=id, symbol=symbol)

    def fetch_open_orders(self, symbol=None, since=None, limit=None):
        return self._account.fetch_open_orders(symbol=symbol, since=since,
                                               limit=limit)

    def fetch_closed_orders(self, symbol=None, since=None, limit=None):
        return self._account.fetch_closed_orders(symbol=symbol, since=since,
                                                 limit=limit)

    def fetch_ticker(self, symbol):
        ohlcv = self._ohlcvs.get(symbol)
        if ohlcv is None:
            raise BadSymbol('ExchangeBackend: no prices for {}'.format(symbol))
        current_date = self._timeframe.date().floor('1T')
        row = ohlcv.loc[current_date]
        timestamp = int(current_date.value / 10**6)
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': Exchange.iso8601(timestamp),
            'high': row['high'],
            'low': row['low'],
            'bid': None,
            'bidVolume': None,
            'ask': None,
            'askVolume': None,
            'vwap': None,
            'open': row['open'],
            'close': row['close'],
            'last': None,
            'previousClose': None,
            'change': None,
            'percentage': None,
            'average': None,
            'baseVolume': None,
            'quoteVolume': None,
            'info': {},
        }

    def fetch_ohlcv_dataframe(self, symbol, timeframe='1m', since=None,
                              limit=None, params={}):
        # Exchanges in the real world have different behaviour, when there is
        # no since parameter provided. (some use data from the beginning,
        # some from the end)
        # We return data from the beginning, because this is most likely not
        # what the user wants, so this will force the user to provide the
        # parameters, which will work with every exchange. This is a bug
        # prevention mechanism.
        ohlcv = self._ohlcvs.get(symbol)
        if ohlcv is None:
            raise BadSymbol('ExchangeBackend: no prices for {}'.format(symbol))
        pd_current_date = self._timeframe.date().floor('1T')
        if limit is None:
            limit = 5
        timeframe_sec = Exchange.parse_timeframe(timeframe)
        pd_timeframe = pandas.Timedelta(timeframe_sec, unit='s')
        ohlcv_start_date = ohlcv.index[0]
        if since is None:
            pd_since = ohlcv_start_date
        else:
            pd_since = pandas.Timestamp(since, unit='ms', tz='UTC')
        pd_since = pd_since.ceil(pd_timeframe)
        if pd_since < ohlcv_start_date:
            raise BadRequest('ExchangeBackend: fetch_ohlcv: no date availabe '
                             'at since')
        pd_until = pd_since + limit * pd_timeframe - pandas.Timedelta('1m')
        if pd_until >= pd_current_date + pd_timeframe:
            raise BadRequest(
                'ExchangeBackend: fetch_ohlcv:'
                ' since.ceil(timeframe) + limit * timeframe'
                ' needs to be in the past')
        pd_until = min(pd_until, pd_current_date)
        data = ohlcv[pd_since:pd_until]
        return data.resample(pd_timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'})
