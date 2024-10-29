import unittest

from mymodule import square, double

class TestSquare(unittest.TestCase):
    def test1(self):
        self.assertEqual(square(2), 4)
        self.assertEqual(square(3.0), 9.0)
        self.assertEqual(square(-3), 9)

class TestDouble(unittest.TestCase):
    def test1(self):
        self.assertEqual(double(2), 4)
        self.assertEqual(double(-3.1), -6.2)
        self.assertEqual(double(0), 0)

unittest.main()