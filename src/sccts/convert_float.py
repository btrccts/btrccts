from ccxt.base.errors import BadRequest
from decimal import Decimal, InvalidOperation


def _convert_float_or_raise(f, msg):
    try:
        val = _convert_float(f)
    except InvalidOperation:
        raise BadRequest('{} needs to be a number'.format(msg))
    if not val.is_finite():
        raise BadRequest('{} needs to be finite'.format(msg))
    return val


def _convert_float(f):
    return Decimal(str(f))
