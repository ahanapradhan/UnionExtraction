import unittest

from mysite.unmasque.refactored.filter import Filter
from mysite.unmasque.refactored.view_minimizer import ViewMinimizer
from mysite.unmasque.test.util import queries, tpchSettings
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_join_graph_and_filter(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q18_test']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q18_test)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q18_test)
        # print(filters)
        self.assertEqual(len(filters), 1)
        f = filters[0]
        self.assertEqual(f[0], 'part')
        self.assertEqual(f[1], 'p_size')
        self.assertEqual(f[2], ">=")
        self.assertEqual(f[3], 4)
        self.conn.closeConnection()

    def test_join_graph_and_filter1(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q18_test1']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q18_test1)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q18_test1)
        print(filters)
        self.assertEqual(len(filters), 2)
        f = filters[0]
        self.assertEqual(f[0], 'part')
        self.assertEqual(f[1], 'p_size')
        self.assertEqual(f[2], ">=")
        self.assertEqual(f[3], 4)

        f = filters[1]
        self.assertEqual(f[0], 'part')
        self.assertEqual(f[1], 'p_retailprice')
        self.assertEqual(f[2], "range")
        self.assertTrue(f[3] > 800)
        self.assertTrue(f[4] < 1000)

        self.conn.closeConnection()

    def test_join_graph_and_filterQ3(self):
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)

        from_rels = tpchSettings.from_rels['Q3']
        minimizer = ViewMinimizer(self.conn, from_rels, tpchSettings.all_size, False)
        check = minimizer.doJob(queries.Q3)
        self.assertTrue(check)

        wc = Filter(self.conn, from_rels, minimizer.global_min_instance_dict)

        filters = wc.doJob(queries.Q3)
        print(filters)
        self.assertEqual(len(filters), 3)
        f = filters[0]
        self.assertEqual(f[0], 'customer')
        self.assertEqual(f[1], 'c_mktsegment')
        self.assertEqual(f[2], "equal")
        self.assertTrue('BUILDING' in f[3])

        f = filters[1]
        self.assertEqual(f[0], 'orders')
        self.assertEqual(f[1], 'o_orderdate')
        self.assertEqual(f[2], "<=")

        f = filters[2]
        self.assertEqual(f[0], 'lineitem')
        self.assertEqual(f[1], 'l_shipdate')
        self.assertEqual(f[2], ">=")

        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
