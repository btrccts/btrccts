from btrccts import parse_params_and_execute_algorithm, AlgorithmBase


class Algorithm(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        # Here you can add additional arguments to the argparser
        argparser.add_argument('--pyramiding', default=1, type=int)

    def __init__(self, context, args):
        # Context is used to create exchanges or get the current time
        self._context = context
        self._args = args

        # This will create a kraken exchange instance
        # The interface in backtesting and live mode is identical to CCXT.
        # See: [CCXT](https://github.com/ccxt/ccxt/wiki)
        # In live mode, this will be a plain ccxt instance of the exchange
        # The exchange keys will be read from the config directory (see --help)
        # You can create sync or async versions of the exchange.
        # If ccxtpro is available in your python environment, the async
        # call will create a ccxtpro instance.
        self._kraken = context.create_exchange('kraken', async_ccxt=True)

        # You can access your own defined parameters
        print('Pyramiding:', args.pyramiding)

        # You can access predefined parameters like exchanges and symbols
        print('Exchanges:', args.exchanges)
        print('Symbols:', args.symbols)

    async def next_iteration(self):
        # This method is executed each time interval
        # This method can be async or a normal method.

        # This is the current context date:
        print('context date', self._context.date())

        # In live mode, markets are not loaded by the library
        # If you need access to the exchanges market object, you need
        # to load them first
        await self._kraken.load_markets()
        # Use the exchange to load OHLCV data
        ohlcv_len = 10
        ohlcv_offset = ohlcv_len * 60 * 1000
        ohlcv_start = int(self._context.date().value / 1000000 - ohlcv_offset)
        print(await self._kraken.fetch_ohlcv(
            'BTC/USD', '1m', ohlcv_start, ohlcv_len))

        # Use the exchange to create a market order
        self._order_id = self._kraken.create_order(
            type='market', side='buy', symbol='BTC/USD', amount=0.1)

        # If you want to stop the algorithm in context or live mode, you can
        # do this:
        self._context.stop('stop message')

    async def handle_exception(self, e):
        # This method is called, when next_iteration raises an exception, e.g.
        # because of an exchange error or a programming error.
        # If this method raises an exception, the algorith will stop with
        # reason EXCEPTION
        # This method can be async or a normal method.
        # If you are not in live mode, it is advicable to rethrow the
        # exception to fix the programming error.
        print(e)
        if not self._args.live:
            raise e

    async def exit(self, reason):
        # This method is called, when the algorithm exits and should be used
        # to cleanup (e.g. cancel open orders).
        # This method can be async or a normal method.
        # reason contains information on why the algorithm exits.
        # e.g. STOPPED, EXCEPTION, FINISHED
        print("Done", reason)
        self.closed_orders = await self._kraken.fetch_closed_orders()
        # Async versions of an exchange needs to be closed, because
        # btrccts will close the asyncio loop after the run.
        await self._kraken.close()


# This method parses commandline parameters (see --help)
# and runs the Algorithm according to the parameters
result = parse_params_and_execute_algorithm(Algorithm)
# The result is an instance of Algorithm, you can now use saved
# information. If you used a sync version of the exchange you can
# still use them. For async exchages the asyncio loop is already
# destroyed.
print(result.orders)
