import pandas
from btrccts.algorithm import AlgorithmBase
from btrccts.run import parse_params_and_execute_algorithm


class Algorithm(AlgorithmBase):

    @staticmethod
    def configure_argparser(argparser):
        pass

    def __init__(self, context, args):
        self._context = context

    def next_iteration(self):
        print('context date', self._context.date())
        print('date', pandas.Timestamp.now(tz='UTC'))
        print('')

    def exit(self, reason):
        print("Done", reason)


parse_params_and_execute_algorithm(Algorithm)
