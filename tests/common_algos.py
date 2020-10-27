from btrccts.algorithm import AlgorithmBase


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
        self.okex = context.create_exchange('okex')

    def next_iteration(self):
        self.iterations += 1
        if self.iterations == 1:
            self.okex.create_order(type='market', side='sell',
                                   symbol='ETH/BTC', amount=2)
        if self.iterations == 4:
            self.kraken.create_order(type='market', side='buy',
                                     symbol='BTC/USD', amount=0.1)

    def exit(self, reason):
        self.exit_reason = reason
