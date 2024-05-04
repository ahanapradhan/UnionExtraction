import unittest

import pytest

from ...src.core import algorithm1
from ...src.core.union_from_clause import UnionFromClause
from ...src.pipeline.UnionPipeLine import UnionPipeLine
from ...test.util import queries
from ...test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = UnionPipeLine(self.conn)

    def test_nonUnion_query(self):
        key = 'Q1'
        query = queries.queries_dict[key]
        u_Q = self.pipeline.doJob(query)
        self.assertTrue(u_Q is not None)
        print(u_Q)
        self.pipeline.time_profile.print()

    def test_nonUnion_query_Q2(self):
        key = 'Q3'
        query = queries.queries_dict[key]
        u_Q = self.pipeline.doJob(query)
        self.assertTrue(u_Q is not None)
        self.assertTrue(self.pipeline.correct)
        print(u_Q)
        self.pipeline.time_profile.print()

    def test_main_cmd_query(self):
        query = "SELECT c_custkey as order_id, COUNT(*) AS total FROM " \
                "customer, orders where c_custkey = o_custkey and o_orderdate >= '1995-01-01' GROUP BY c_custkey " \
                "ORDER BY total ASC LIMIT 10;"
        u_Q = self.pipeline.doJob(query)
        self.assertTrue(u_Q is not None)
        self.assertTrue(self.pipeline.correct)
        print(u_Q)
        self.pipeline.time_profile.print()

    def test_nonUnion_queries(self):
        Q_keys = queries.queries_dict.keys()
        q_no = 1
        for q_key in Q_keys:
            query = queries.queries_dict[q_key]
            u_Q = self.pipeline.doJob(query)
            self.assertTrue(u_Q is not None)
            self.assertTrue(self.pipeline.correct)
            print(u_Q)

            self.pipeline.time_profile.print()
            q_no += 1

    def test_unionQ(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= " \
                "905) " \
                "union all " \
                "(select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);"
        u_Q = self.pipeline.doJob(query)
        self.assertTrue(u_Q is not None)
        print(u_Q)
        self.pipeline.time_profile.print()

    def test_unionQuery_ui_caught_case(self):
        self.conn.connectUsingParams()
        query = "(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and " \
                "n_name = 'UNITED STATES') UNION ALL " \
                "(SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey " \
                "and l_quantity > 35);"

        db = UnionFromClause(self.conn)
        p, pstr, _ = algorithm1.algo(db, query)
        self.assertEqual(p, {frozenset({'customer', 'nation'}), frozenset({'part', 'lineitem'})})
        self.assertTrue(pstr is not None)
        self.conn.closeConnection()

    def test_nonUnion_outerJoin(self):
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_unionQuery_ui_caught_case_outerJoin(self):
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        query = f"select c_name, n_comment FROM customer FULL OUTER JOIN nation on c_nationkey = " \
                f"n_nationkey and c_acctbal < 2000 " \
                f"UNION ALL" \
                f" select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_random_nonUnion(self):
        query = "SELECT o_orderdate, SUM(l_extendedprice) AS total_price " \
                "FROM orders, lineitem where o_orderkey = l_orderkey " \
                "and o_orderdate <= '1995-01-01' GROUP BY o_orderdate " \
                "ORDER BY total_price DESC LIMIT 10;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_another(self):
        query = "SELECT l_orderkey as key, l_quantity as dummy, " \
                "l_partkey as s_key FROM lineitem WHERE l_shipdate >= DATE '1994-01-01'" \
                " AND l_shipdate < DATE '1995-01-01' " \
                "AND l_quantity > 30 UNION ALL SELECT " \
                "ps_partkey as key, ps_supplycost as dummy, " \
                "ps_suppkey as s_key FROM partsupp, orders WHERE" \
                " partsupp.ps_suppkey = orders.o_custkey " \
                "AND orders.o_orderdate >= DATE '1994-01-01' AND orders.o_orderdate < DATE '1995-01-01' " \
                "AND partsupp.ps_supplycost < 100;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_paper_example(self):
        query = "SELECT c_name as name, c_acctbal as account_balance " \
                "FROM orders, customer, nation WHERE c_custkey = o_custkey " \
                "and c_nationkey = n_nationkey and c_mktsegment = 'FURNITURE' " \
                "and n_name = 'INDIA' " \
                "and o_orderdate between '1998-01-01' and '1998-01-05' " \
                "and o_totalprice <= c_acctbal " \
                "UNION ALL SELECT s_name as name, " \
                "s_acctbal as account_balance " \
                "FROM supplier, lineitem, orders, nation " \
                "WHERE l_suppkey = s_suppkey " \
                "and l_orderkey = o_orderkey " \
                "and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
                "and o_orderdate between '1998-01-01' and '1998-01-05' " \
                "and o_totalprice >= s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_UQ11(self):
        self.conn.connectUsingParams()
        query = "Select o_orderpriority, " \
                "count(*) as order_count " \
                "From orders, lineitem " \
                "Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' " \
                "and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate " \
                "Group By o_orderpriority " \
                "Order By o_orderpriority;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_UQ10(self):
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate " \
                "and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' " \
                "and l_extendedprice <= o_totalprice " \
                "and l_extendedprice <= 70000 " \
                "and o_totalprice > 60000 " \
                "Group By l_shipmode " \
                "Order By l_shipmode;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_outer_join(self):
        query = "select n_name from nation FULL OUTER JOIN region on n_regionkey = r_regionkey and r_name = 'AFRICA'" \
                " UNION ALL " \
                "select n_name from nation RIGHT OUTER JOIN customer on " \
                "c_nationkey = n_nationkey and c_acctbal < 1000;"
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        self.conn.config.detect_or = False
        self.conn.config.detect_nep = False

        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
