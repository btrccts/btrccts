from ccxt.base.errors import InvalidOrder, BadRequest


class AsyncBacktestExchangeBase:

    def __init__(self, config, exchange_backend):
        super().__init__(config=config)
        self._exchange_backend = exchange_backend

    def _check_has(self, name):
        if not self.has[name]:
            raise NotImplementedError('{}: method not implemented: {}'
                                      .format(self.id, name))

    async def cancel_all_orders(self, *args, **kwargs):
        self._check_has('cancelAllOrders')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'cancel_all_orders')

    async def cancel_order(self, id, symbol=None, params={}):
        self._check_has('cancelOrder')
        return self._exchange_backend.cancel_order(id=id, symbol=symbol)

    async def cancel_orders(self, *args, **kwargs):
        self._check_has('cancelOrders')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'cancel_orders')

    async def create_deposit_address(self, *args, **kwargs):
        self._check_has('createDepositAddress')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'create_deposit_address')

    async def create_order(self, symbol, type, side, amount, price=None,
                           params={}):
        self._check_has('createOrder')
        await super().load_markets()
        if type == 'market':
            if not self.has['createMarketOrder']:
                raise NotImplementedError(
                    '{}: method not implemented: createMarketOrder'
                    .format(self.id))
        elif type == 'limit':
            if not self.has['createLimitOrder']:
                raise NotImplementedError(
                    '{}: method not implemented: createLimitOrder'
                    .format(self.id))
        market = self.markets.get(symbol)
        if market is None:
            raise InvalidOrder('Exchange: market does not exist: {}'.format(
                symbol))
        return self._exchange_backend.create_order(
            market=market, side=side, amount=amount, type=type, price=price)

    async def deposit(self, *args, **kwargs):
        self._check_has('deposit')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'deposit')

    async def edit_order(self, *args, **kwargs):
        self._check_has('editOrder')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'edit_order')

    async def fetch_balance(self, params={}):
        self._check_has('fetchBalance')
        result = self._exchange_backend.fetch_balance()
        return self.parse_balance(result)

    async def fetch_closed_orders(
            self, symbol=None, since=None, limit=None, params={}):
        self._check_has('fetchClosedOrders')
        return self._exchange_backend.fetch_closed_orders(
            symbol=symbol, since=since, limit=limit)

    async def fetch_currencies(self, params={}):
        self._check_has('fetchCurrencies')
        return await super().fetch_currencies(params)

    async def fetch_deposit_address(self, *args, **kwargs):
        self._check_has('fetchDepositAddress')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_deposit_address')

    async def fetch_deposits(self, *args, **kwargs):
        self._check_has('fetchDeposits')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_deposits')

    async def fetch_l2_order_book(self, *args, **kwargs):
        self._check_has('fetchL2OrderBook')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_l2_order_book')

    async def fetch_ledger(self, *args, **kwargs):
        self._check_has('fetchLedger')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_ledger')

    async def fetch_markets(self, params={}):
        self._check_has('fetchMarkets')
        return await super().fetch_markets(params)

    async def fetch_my_trades(self, *args, **kwargs):
        self._check_has('fetchMyTrades')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_my_trades')

    async def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None,
                          params={}):
        self._check_has('fetchOHLCV')
        if timeframe not in self.timeframes:
            raise BadRequest('Timeframe {} not supported by exchange'.format(
                timeframe))
        data = self._exchange_backend.fetch_ohlcv_dataframe(
            symbol=symbol, timeframe=timeframe, since=since, limit=limit)
        result = [[int(values.Index.value / 10**6),
                   values.open,
                   values.high,
                   values.low,
                   values.close,
                   values.volume] for values in data.itertuples()]
        return result

    async def fetch_open_orders(
            self, symbol=None, since=None, limit=None, params={}):
        self._check_has('fetchOpenOrders')
        return self._exchange_backend.fetch_open_orders(
            symbol=symbol, since=since, limit=limit)

    async def fetch_order(self, id, symbol=None, params={}):
        self._check_has('fetchOrder')
        return self._exchange_backend.fetch_order(id=id, symbol=symbol)

    async def fetch_order_book(self, *args, **kwargs):
        self._check_has('fetchOrderBook')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_order_book')

    async def fetch_order_books(self, *args, **kwargs):
        self._check_has('fetchOrderBooks')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_order_books')

    async def fetch_orders(self, *args, **kwargs):
        self._check_has('fetchOrders')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_orders')

    async def fetch_status(self, *args, **kwargs):
        self._check_has('fetchStatus')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_status')

    async def fetch_ticker(self, symbol, params={}):
        self._check_has('fetchTicker')
        return self._exchange_backend.fetch_ticker(symbol=symbol)

    async def fetch_tickers(self, *args, **kwargs):
        self._check_has('fetchTickers')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_tickers')

    async def fetch_time(self, *args, **kwargs):
        self._check_has('fetchTime')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_time')

    async def fetch_trades(self, *args, **kwargs):
        self._check_has('fetchTrades')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trades')

    async def fetch_trading_fee(self, *args, **kwargs):
        self._check_has('fetchTradingFee')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_fee')

    async def fetch_trading_fees(self, *args, **kwargs):
        self._check_has('fetchTradingFees')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_fees')

    async def fetch_funding_fee(self, *args, **kwargs):
        self._check_has('fetchFundingFee')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_funding_fee')

    async def fetch_funding_fees(self, *args, **kwargs):
        self._check_has('fetchFundingFees')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_funding_fees')

    async def fetch_trading_limits(self, *args, **kwargs):
        self._check_has('fetchTradingLimits')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_limits')

    async def fetch_transactions(self, *args, **kwargs):
        self._check_has('fetchTransactions')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_transactions')

    async def fetch_withdrawals(self, *args, **kwargs):
        self._check_has('fetchWithdrawals')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_withdrawals')

    async def withdraw(self, *args, **kwargs):
        self._check_has('withdraw')
        raise NotImplementedError('BacktestExchange does not support method '
                                  'withdraw')
