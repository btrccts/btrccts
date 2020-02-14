# btrccts

Simulate CryptoCurrency Trading Strategies

## Development setup

Setup a virtualenv:

    python3 -m venv .venv
    # TODO: provide requirements file with hashes
    .venv/bin/pip install -e .

## Run tests

    .venv/bin/python -m unittest tests/unit/tests.py
    .venv/bin/python -m unittest tests/integration/tests.py
