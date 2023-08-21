import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.src.core import MinusPipeLine


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_something(self):
        query = "(SELECT c_name as name, o_totalprice as balance, c_phone as phone " \
                "FROM   customer, lineitem, orders " \
                "WHERE  l_orderkey = o_orderkey " \
                "and o_custkey = c_custkey " \
                "and o_orderdate > DATE '1996-01-03' " \
                "and l_quantity >= 50 " \
                "and l_shipinstruct = 'COLLECT COD'" \
                "Group By o_totalprice, c_name, c_phone) " \
                "EXCEPT " \
                "(SELECT c_name as name, s_acctbal as balance, s_phone as phone " \
                "FROM   supplier, customer, nation " \
                "WHERE  s_nationkey = n_nationkey " \
                "and c_nationkey = s_nationkey " \
                "and c_mktsegment = 'MACHINERY' " \
                "and c_phone like '25%');"

        Q_E, comtabs = MinusPipeLine.extract(self.conn, query)
        self.assertTrue(len(comtabs) > 0)
        print(Q_E)


if __name__ == '__main__':
    unittest.main()
