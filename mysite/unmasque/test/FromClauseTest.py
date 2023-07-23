import unittest

from mysite.unmasque.refactored import from_clause
from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper


class MyTestCase(unittest.TestCase):
    tpch_query1 = "select count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval ':1' " \
                  "day group by l_returnflag, l_linestatus;"

    tpch_query3 = "select c_mktsegment, l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, " \
                  "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
                  "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
                  "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"

    def test_something(self):
        query = self.tpch_query1
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        rels = from_clause.getCoreRelations(conn, query, "error")
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)

    def test_something1(self):
        query = self.tpch_query3
        conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")
        conn.connectUsingParams()
        self.assertTrue(conn.conn is not None)
        rels = from_clause.getCoreRelations(conn, query, "error")
        self.assertEqual(len(rels), 3)
        self.assertTrue('lineitem' in rels)
        self.assertTrue('customer' in rels)
        self.assertTrue('orders' in rels)





if __name__ == '__main__':
    unittest.main()
