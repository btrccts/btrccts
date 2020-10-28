class AlgorithmBase:

    @staticmethod
    def configure_argparser(argparser):
        pass

    def __init__(self, context, args):
        pass

    async def exit(self, reason):
        pass

    async def handle_exception(self, e):
        raise e

    async def next_iteration(self):
        pass
