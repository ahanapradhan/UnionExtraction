import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core.n_minimizer import NMinimizer
from mysite.unmasque.test.util import tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    tab_customer = "customer"
    tab_nation = "nation"
    tab_supplier = "supplier"
    tab_orders = "orders"
    tab_lineitem = "lineitem"

    def test_for_single_relation(self):
        self.conn.connectUsingParams()
        nm = NMinimizer(self.conn, [self.tab_nation], tpchSettings.all_size)
        nm.mock = True
        query = "select * from nation where n_name = 'EGYPT';"
        print(query)
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_nation])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
        self.conn.closeConnection()

    def test_for_multiple_relations(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer]
        # relations = [self.tab_customer]
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
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
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
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
        self.conn.closeConnection()

    def test_adonis_case_1(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer, self.tab_supplier]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT n_name FROM customer, nation WHERE c_nationkey = n_nationkey AND c_acctbal > 4000 " \
                "INTERSECT " \
                "SELECT n_name FROM supplier, nation WHERE s_nationkey = n_nationkey AND s_acctbal > 4000;"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_customer])
        self.assertEqual(1, nm.core_sizes[self.tab_supplier])
        self.assertEqual(2, nm.core_sizes[self.tab_nation])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
        self.conn.closeConnection()

    def test_adonis_case_2_simple_on_orders(self):
        self.conn.connectUsingParams()
        relations = [self.tab_orders, self.tab_lineitem]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT o_totalprice as price FROM orders , lineitem WHERE o_orderkey = l_orderkey " \
                "and o_orderdate > DATE '1995-01-01' AND l_shipdate > DATE '1995-01-01';"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_orders])
        self.assertEqual(1, nm.core_sizes[self.tab_lineitem])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
        self.conn.closeConnection()

    def test_adonis_case_2(self):
        self.case2()
        # pass

    def case2(self):
        self.conn.connectUsingParams()
        relations = [self.tab_orders, self.tab_customer, self.tab_lineitem]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT c_acctbal as price FROM customer, orders WHERE c_custkey = o_custkey " \
                "AND o_orderdate > DATE '1995-01-01' INTERSECT " \
                "SELECT o_totalprice as price FROM orders, lineitem WHERE o_orderkey = l_orderkey " \
                "AND l_shipdate > DATE '1995-01-01';"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_customer])
        self.assertEqual(1, nm.core_sizes[self.tab_lineitem])
        self.assertEqual(2, nm.core_sizes[self.tab_orders])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        nm.see_d_min()
        self.conn.closeConnection()

    def test_adonis_case_1(self):
        self.conn.connectUsingParams()
        relations = [self.tab_nation, self.tab_customer, self.tab_supplier]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT n_name FROM customer, nation WHERE c_nationkey = n_nationkey AND c_acctbal > 4000 " \
                "INTERSECT " \
                "SELECT n_name FROM supplier, nation WHERE s_nationkey = n_nationkey AND s_acctbal > 4000;"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_customer])
        self.assertEqual(1, nm.core_sizes[self.tab_supplier])
        self.assertEqual(2, nm.core_sizes[self.tab_nation])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(res))
        self.conn.closeConnection()

    def test_adonis_case_2_simple_on_orders(self):
        self.conn.connectUsingParams()
        relations = [self.tab_orders, self.tab_lineitem]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT o_totalprice as price FROM orders , lineitem WHERE o_orderkey = l_orderkey " \
                "and o_orderdate > DATE '1995-01-01' AND l_shipdate > DATE '1995-01-01';"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_orders])
        self.assertEqual(1, nm.core_sizes[self.tab_lineitem])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(query))
        self.conn.closeConnection()

    def test_adonis_case_2(self):
        self.conn.connectUsingParams()
        relations = [self.tab_orders, self.tab_customer, self.tab_lineitem]
        nm = NMinimizer(self.conn, relations, tpchSettings.all_size)
        nm.mock = True
        query = "SELECT c_acctbal as price FROM customer, orders WHERE c_custkey = o_custkey " \
                "AND o_orderdate > DATE '1995-01-01' INTERSECT " \
                "SELECT o_totalprice as price FROM orders, lineitem WHERE o_orderkey = l_orderkey " \
                "AND l_shipdate > DATE '1995-01-01';"
        check = nm.doJob(query)
        self.assertTrue(check)
        self.assertEqual(1, nm.core_sizes[self.tab_customer])
        self.assertEqual(1, nm.core_sizes[self.tab_lineitem])
        self.assertEqual(2, nm.core_sizes[self.tab_orders])
        app = Executable(self.conn)
        res = app.doJob(query)
        self.assertTrue(not isQ_result_empty(query))
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
