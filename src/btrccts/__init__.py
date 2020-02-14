import sys
import os
import importlib.util
from btrccts.run import parse_params_and_execute_algorithm
from btrccts.algorithm import AlgorithmBase  # noqa
from unittest.mock import patch


__all__ = ['AlgorithmBase', 'parse_params_and_execute_algorithm']


# TODO: Test these functions
def _load_algorithm_from_file(filepath):
    spec = importlib.util.spec_from_file_location("Algorithm", filepath)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    Algorithm = getattr(foo, 'Algorithm')
    return Algorithm


def _main():
    if len(sys.argv) < 2:
        print('File to load needs to be first parameter')
        sys.exit(1)

    argv_mod = sys.argv.copy()
    filepath = argv_mod[1]
    sys.path.append(os.path.abspath(os.path.dirname(filepath)))
    AlgorithmClass = _load_algorithm_from_file(filepath)
    del argv_mod[1]
    with patch.object(sys, 'argv', argv_mod):
        parse_params_and_execute_algorithm(AlgorithmClass)


if __name__ == "__main__":
    _main()
