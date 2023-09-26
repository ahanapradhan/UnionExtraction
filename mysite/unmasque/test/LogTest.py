import unittest

from mysite.unmasque.src.util.Log import Log


class MyTestCase(unittest.TestCase):
    log = Log("Test Case", 'ERROR')

    def func_args(self, *args):
        return "%s" % args

    def test_something(self):
        self.log.error("hello", 'world', 'hey', "bye")


if __name__ == '__main__':
    unittest.main()
