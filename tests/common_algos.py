from btrccts.algorithm import AlgorithmBase
from btrccts.run import ExitReason


class TestAlgo(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        argparser.add_argument('--algo-bool', action='store_true')
        argparser.add_argument('--some-string', default='')

    def __init__(self, context, args):
        self.args = args
        self.exit_reason = None
        self.iterations = 0
        self.kraken = context.create_exchange('kraken')
        self.okx = context.create_exchange('okx')
        self.okx_balance = None
        self.kraken_balance = None
        self.exception = None
        self.exception_iteration = None

    def next_iteration(self):
        self.iterations += 1
        if self.iterations == 1:
            self.okx.create_order(type='market', side='sell',
                                  symbol='ETH/BTC', amount=2)
        if self.iterations == 2:
            raise ValueError('test')
        if self.iterations == 4:
            self.kraken.create_order(type='market', side='buy',
                                     symbol='BTC/USD', amount=0.1)

    def handle_exception(self, exp):
        self.exception = exp
        self.exception_iteration = self.iterations

    def exit(self, reason):
        self.exit_reason = reason
        self.okx_balance = self.okx.fetch_balance()
        self.kraken_balance = self.kraken.fetch_balance()


class AsyncTestAlgo(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        argparser.add_argument('--algo-bool', action='store_true')
        argparser.add_argument('--some-string', default='')

    def __init__(self, context, args):
        self.args = args
        self.exit_reason = None
        self.iterations = 0
        self.kraken = context.create_exchange('kraken', async_ccxt=True)
        self.okx = context.create_exchange('okx', async_ccxt=True)
        self.okx_balance = None
        self.kraken_balance = None
        self.exception = None
        self.exception_iteration = None

    async def next_iteration(self):
        self.iterations += 1
        if self.iterations == 1:
            await self.okx.create_order(type='market', side='sell',
                                        symbol='ETH/BTC', amount=2)
        if self.iterations == 2:
            raise ValueError('test')
        if self.iterations == 4:
            await self.kraken.create_order(type='market', side='buy',
                                           symbol='BTC/USD', amount=0.1)

    async def handle_exception(self, exp):
        self.exception = exp
        self.exception_iteration = self.iterations

    async def exit(self, reason):
        self.exit_reason = reason
        self.okx_balance = await self.okx.fetch_balance()
        self.kraken_balance = await self.kraken.fetch_balance()
        await self.kraken.close()
        await self.okx.close()


def assert_test_algo_result(test, result, live=False, async_algo=False):
    if async_algo:
        test.assertEqual(type(result), AsyncTestAlgo)
    else:
        test.assertEqual(type(result), TestAlgo)
    test.assertEqual(result.exit_reason, ExitReason.FINISHED)
    test.assertEqual(result.iterations, 4)
    if live:
        okx_balance = {'BTC': 199.40045, 'ETH': 1.0}
        kraken_balance = {'BTC': 0.0996, 'USD': 99.09865}
    else:
        okx_balance = {'BTC': 197.703, 'ETH': 1.0}
        kraken_balance = {'BTC': 0.0998, 'USD': 99.09865}
    test.assertEqual(result.okx_balance['total'], okx_balance)
    test.assertEqual(result.kraken_balance['total'], kraken_balance)
    test.assertEqual(result.exception_iteration, 2)
    test.assertEqual(type(result.exception), ValueError)
    test.assertEqual(result.exception.args, ('test',))
