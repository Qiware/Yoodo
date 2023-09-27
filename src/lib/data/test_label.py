import unittest

from label import Label


class TestLabel(unittest.TestCase):
    def test_ratio(self):
        label = Label()

        self.assertEqual(0, label.ratio(0, 0))
        self.assertEqual(-100, label.ratio(1, 0))
        self.assertEqual(100, label.ratio(0, 1))
        self.assertEqual(-90, label.ratio(10, 1))
        self.assertEqual(900, label.ratio(1, 10))

    def test_gen_classify(self):
        label = Label()

        self.assertEqual(10, label.gen_classify(10.1))
        self.assertEqual(0, label.gen_classify(0.1))
        self.assertEqual(-1, label.gen_classify(-0.1))
        self.assertEqual(-11, label.gen_classify(-10.1))

    def test_str2label(self):
        label = Label()

        stock_key = "hkex:00700"
        digit = label.str2label(stock_key)

        print("key:%s label:%d" % (stock_key, digit))

        self.assertTrue(digit > 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
