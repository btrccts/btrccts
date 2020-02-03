import sys
import os
from sccts.run import parse_params_and_execute_algorithm
from sccts.algorithm import AlgorithmBase
from unittest.mock import patch


def _load_algorithm_from_file(filepath):
    with open(sys.argv[1]) as f:
        source = f.read()
    file_scope = {'AlgorithmBase': AlgorithmBase}
    exec(source, {}, file_scope)
    Algorithm = file_scope.get('Algorithm')
    if Algorithm is not None:
        return Algorithm
    raise ValueError('The file {} needs to contain the Algorithm class '
                     'called Algorithm'.format(filepath))


# Warning: this is not documented and not officially supported
# There is a problem, global imports are not supported via the exec approach
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
