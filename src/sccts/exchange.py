from ccxt.base.errors import InvalidOrder


def check_has(name):

    def decorator(func):
        def wrapper(self, *args, **kwargs):
            if not self.has[name]:
                raise NotImplementedError('{}: method not implemented: {}'
                                          .format(self.id, name))
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


class BacktestExchangeBase:

    def __init__(self, config, exchange_backend):
        super().__init__(config=config)
        self._exchange_backend = exchange_backend

    @check_has('cancelAllOrders')
    def cancel_all_orders(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'cancel_all_orders')

    @check_has('cancelOrder')
    def cancel_order(self, id, symbol=None, params={}):
        return self._exchange_backend.cancel_order(id=id, symbol=symbol)

    @check_has('cancelOrders')
    def cancel_orders(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'cancel_orders')

    @check_has('createDepositAddress')
    def create_deposit_address(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'create_deposit_address')

    @check_has('createOrder')
    def create_order(self, symbol, type, side, amount, price=None, params={}):
        super().load_markets()
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

    @check_has('deposit')
    def deposit(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'deposit')

    @check_has('editOrder')
    def edit_order(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'edit_order')

    @check_has('fetchBalance')
    def fetch_balance(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_balance')

    @check_has('fetchClosedOrders')
    def fetch_closed_orders(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_closed_orders')

    @check_has('fetchCurrencies')
    def fetch_currencies(self, params={}):
        return super().fetch_markets(params)

    @check_has('fetchDepositAddress')
    def fetch_deposit_address(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_deposit_address')

    @check_has('fetchDeposits')
    def fetch_deposits(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_deposits')

    @check_has('fetchL2OrderBook')
    def fetch_l2_order_book(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_l2_order_book')

    @check_has('fetchLedger')
    def fetch_ledger(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_ledger')

    @check_has('fetchMarkets')
    def fetch_markets(self, params={}):
        return super().fetch_markets(params)

    @check_has('fetchMyTrades')
    def fetch_my_trades(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_my_trades')

    @check_has('fetchOHLCV')
    def fetch_ohlcv(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_ohlcv')

    @check_has('fetchOpenOrders')
    def fetch_open_orders(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_open_orders')

    @check_has('fetchOrder')
    def fetch_order(self, id, symbol=None, params={}):
        return self._exchange_backend.fetch_order(id=id, symbol=symbol)

    @check_has('fetchOrderBook')
    def fetch_order_book(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_order_book')

    @check_has('fetchOrderBooks')
    def fetch_order_books(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_order_books')

    @check_has('fetchOrders')
    def fetch_orders(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_orders')

    @check_has('fetchStatus')
    def fetch_status(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_status')

    @check_has('fetchTicker')
    def fetch_ticker(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_ticker')

    @check_has('fetchTickers')
    def fetch_tickers(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_tickers')

    @check_has('fetchTime')
    def fetch_time(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_time')

    @check_has('fetchTrades')
    def fetch_trades(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trades')

    @check_has('fetchTradingFee')
    def fetch_trading_fee(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_fee')

    @check_has('fetchTradingFees')
    def fetch_trading_fees(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_fees')

    @check_has('fetchFundingFee')
    def fetch_funding_fee(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_funding_fee')

    @check_has('fetchFundingFees')
    def fetch_funding_fees(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_funding_fees')

    @check_has('fetchTradingLimits')
    def fetch_trading_limits(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_trading_limits')

    @check_has('fetchTransactions')
    def fetch_transactions(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_transactions')

    @check_has('fetchWithdrawals')
    def fetch_withdrawals(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'fetch_withdrawals')

    @check_has('withdraw')
    def withdraw(self, *args, **kwargs):
        raise NotImplementedError('BacktestExchange does not support method '
                                  'withdraw')
