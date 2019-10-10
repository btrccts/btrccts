class AlgorithmBase:

    @staticmethod
    def configure_parser(argparser):
        pass

    def __init__(self, context, args):
        pass

    def exit(self, reason):
        # reason: 'stopped', 'exception', 'finished'
        pass

    def handle_exception(self, e):
        raise e

    def handle_period(self):
        # TODO There is probably a better name needed
        pass
