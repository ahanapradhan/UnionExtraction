import unittest

from mysite.unmasque.src.core.n_minimizer import NMinimizer
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    tab_customer = "customer"
    tab_nation = "nation"

    def test_for_single_relation(self):
        self.conn.connectUsingParams()
        nm = NMinimizer(self.conn, [self.tab_nation], tpchSettings.all_size)
        nm.mock = True
        query = "select * from nation where n_name = 'EGYPT';"
        print(query)
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_nation])
        self.conn.closeConnection()

    def test_for_multiple_relations(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True

        query = "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'BRAZIL' INTERSECT " \
                "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
                "AND n_name = 'ARGENTINA';"
        print(query)
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(2, nm.core_sizes[self.tab_nation])
        self.assertEqual(2, nm.core_sizes[self.tab_customer])
        self.conn.closeConnection()

    def test_other(self):
        self.conn.connectUsingParams()
        relations = [self.tab_customer]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True

        query = "SELECT max(c_acctbal) FROM customer where c_acctbal < 0.0;"
        print(query)
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_customer])
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
