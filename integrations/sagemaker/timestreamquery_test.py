import unittest
import timestreamquery

class TestStreamQuery(unittest.TestCase):

    def test_bool_true(self):
        self.assertTrue(timestreamquery.toBool('True'))
        self.assertTrue(timestreamquery.toBool('yes'))
        self.assertTrue(timestreamquery.toBool('t'))
        self.assertTrue(timestreamquery.toBool('true'))
        self.assertTrue(timestreamquery.toBool('on'))
        self.assertTrue(timestreamquery.toBool('1'))
        self.assertTrue(timestreamquery.toBool(1))
        self.assertTrue(timestreamquery.toBool(True))

    def test_bool_false(self):
        self.assertFalse(timestreamquery.toBool('Tr'))
        self.assertFalse(timestreamquery.toBool('f'))
        self.assertFalse(timestreamquery.toBool('0'))
        self.assertFalse(timestreamquery.toBool(0))
        self.assertFalse(timestreamquery.toBool('False'))
        self.assertFalse(timestreamquery.toBool(False))


if __name__ == '__main__':
    unittest.main()