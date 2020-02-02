from sccts.algorithm import AlgorithmBase


class Algorithm(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        pass

    def __init__(self, context, args):
        self._context = context

    def next_iteration(self):
        print(self._context.date())

    def exit(self, reason):
        print("Done", reason)
