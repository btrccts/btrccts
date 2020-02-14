import unittest
from ccxt.base.errors import InsufficientFunds
from decimal import Decimal
from btrccts.balance import Balance


class BalanceTest(unittest.TestCase):

    def test__default_init(self):
        balance = Balance()
        self.assertEqual(balance.to_dict(), {
            'free': Decimal(0),
            'used': Decimal(0),
            'total': Decimal(0),
        })

    def test__positive_initialization(self):
        balance = Balance(15.3)
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

    def test__change_total__add(self):
        balance = Balance(15.3)
        balance.change_total(0.7)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('16.0'),
            'used': Decimal(0),
            'total': Decimal('16.0'),
        })

    def test__change_total__substraction(self):
        balance = Balance(15.3)
        balance.change_total(-0.3)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.0'),
            'used': Decimal(0),
            'total': Decimal('15.0'),
        })

    def test__change_total__to_negative(self):
        balance = Balance(15.3)
        with self.assertRaises(InsufficientFunds) as e:
            balance.change_total(-16)
        self.assertEqual(str(e.exception), 'Balance too little')
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__change_used__add(self):
        balance = Balance(15.3)
        balance.change_used(0.5)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('14.8'),
            'used': Decimal(0.5),
            'total': Decimal('15.3'),
        })

    def test__change_used__substraction(self):
        balance = Balance(15.3)
        balance.change_used(-0.5)
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.8'),
            'used': Decimal(-0.5),
            'total': Decimal('15.3'),
        })

    def test__change_used__to_negative(self):
        balance = Balance(15.3)
        with self.assertRaises(InsufficientFunds) as e:
            balance.change_used(16)
        self.assertEqual(str(e.exception), 'Balance too little')
        self.assertEqual(balance.to_dict(), {
            'free': Decimal('15.3'),
            'used': Decimal(0),
            'total': Decimal('15.3'),
        })

    def test__repr(self):
        balance = Balance(15.3)
        balance.change_used(0.5)
        self.assertEqual(str(balance),
                         "{'free': Decimal('14.8'), 'used': Decimal('0.5'), "
                         "'total': Decimal('15.3')}")
