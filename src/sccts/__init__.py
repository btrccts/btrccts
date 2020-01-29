import sys
from sccts.run import parse_params_and_execute_algorithm
from unittest.mock import patch


def _load_algorithm_from_file(filepath):
    with open(sys.argv[1]) as f:
        source = f.read()
    file_scope = {}
    exec(source, {}, file_scope)
    Algorithm = file_scope.get('Algorithm')
    if Algorithm is not None:
        return Algorithm
    raise ValueError('The file {} needs to contain the Algorithm class '
                     'called Algorithm'.format(filepath))


def _main():
    if len(sys.argv) < 2:
        print('File to load needs to be first parameter')
        sys.exit(1)

    argv_mod = sys.argv.copy()
    filepath = argv_mod[1]
    AlgorithmClass = _load_algorithm_from_file(filepath)
    del argv_mod[1]
    with patch.object(sys, 'argv', argv_mod):
        parse_params_and_execute_algorithm(AlgorithmClass)


if __name__ == "__main__":
    _main()
