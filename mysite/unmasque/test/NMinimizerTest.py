import unittest

from mysite.unmasque.src.core.n_minimizer import NMinimizer
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def test_something(self):
        self.conn.connectUsingParams()
        nm = NMinimizer(self.conn, ["nation"], {"nation" : tpchSettings.all_size["nation"]})
        query = "select * from nation where n_name = 'EGYPT';"
        print(query)
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes["nation"])
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
