class AlgorithmBaseSync:

    @staticmethod
    def configure_argparser(argparser):
        pass

    def __init__(self, context, args):
        pass

    def exit(self, reason):
        pass

    def handle_exception(self, e):
        raise e

    def next_iteration(self):
        pass


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
