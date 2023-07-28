import unittest
import timestreamquery

class TestStreamQuery(unittest.TestCase):

    def test_bool_true(self):
        self.assertTrue(timestreamquery.toBool('True'))
        self.assertTrue(timestreamquery.toBool('true'))
        self.assertTrue(timestreamquery.toBool(True))

    def test_bool_false(self):
        self.assertFalse(timestreamquery.toBool('False'))
        self.assertFalse(timestreamquery.toBool('false'))
        self.assertFalse(timestreamquery.toBool(False))

    def test_bool_invalid(self):
        with self.assertRaises(ValueError) as context:
            timestreamquery.toBool('Incorrect')
        self.assertTrue('Error converting value: (Incorrect) to boolean. Allowed values true/false', context.exception)


if __name__ == '__main__':
    unittest.main()
