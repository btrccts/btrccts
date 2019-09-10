import unittest
from decimal import Decimal
from sccts.exchange_backend import Balance


class BalanceTest(unittest.TestCase):

    def test__default_init(self):
        balance = Balance()
        self.assertEqual(balance.free(), 0)
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), 0)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal(0),
            'used': Decimal(0),
            'total': Decimal(0),
        })

    def test__positive_initialization(self):
        balance = Balance(15.3)
        self.assertEqual(balance.free(), Decimal('15.3'))
        self.assertEqual(balance.used(), 0)
        self.assertEqual(balance.total(), Decimal('15.3'))
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__negative_initialization(self):
        with self.assertRaises(ValueError) as e:
            Balance(-1)
        self.assertEqual(str(e.exception),
                         'Balance: inital value cant be negative')
