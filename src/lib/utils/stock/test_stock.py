import unittest

from stock import *


class TestStock(unittest.TestCase):
    def test_get_stock_code_by_key(self):
        self.assertEqual("00700", get_stock_code_by_key("hkex:00700"))
