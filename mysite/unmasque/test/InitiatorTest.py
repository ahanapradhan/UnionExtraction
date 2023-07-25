import unittest

from mysite.unmasque.refactored.initialization import Initiator


class MyTestCase(unittest.TestCase):
    def test_something(self):
        relations = ["orders", "lineitem", "customer", "supplier", "part", "partsupp", "nation", "region"]
        initor = Initiator(None, relations)
        initor.doJob("")
        print(initor.global_index_dict)
        print(initor.global_key_lists)
        print(initor.global_pk_dict)


if __name__ == '__main__':
    unittest.main()
