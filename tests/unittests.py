import unittest
from tests.pep_checker import Pep8Test


def test_suite():
    suite = unittest.TestSuite([
        unittest.makeSuite(Pep8Test),
    ])
    return suite
