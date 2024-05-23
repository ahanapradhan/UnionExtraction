import unittest

from ...src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from ...test.util.BaseTestCase import BaseTestCase
import pytest


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_basic_simple(self):
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where " \
                "o_orderkey = l_orderkey " \
                "and l_extendedprice <= 70000 " \
                "and o_totalprice >= 60000 " \
                "and l_extendedprice <= o_totalprice " \
                "Group By l_shipmode " \
                "Order By l_shipmode;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_basic(self):
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where " \
                "o_orderkey = l_orderkey " \
                "and l_receiptdate >= '1994-01-01' " \
                "and l_receiptdate <= '1995-01-01' " \
                "and l_extendedprice <= 70000 " \
                "and o_totalprice >= 60000 " \
                "Group By l_shipmode " \
                "Order By l_shipmode;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_dormant_aoa(self):
        query = "Select l_shipmode, count(*) as count From orders, lineitem " \
                "Where o_orderkey = l_orderkey and l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate " \
                "and l_receiptdate >= '1994-01-01' and l_receiptdate <= '1995-01-01' and l_extendedprice < " \
                "o_totalprice and l_extendedprice <= 70000 and o_totalprice >= 60000 Group By l_shipmode " \
                "Order By l_shipmode;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ10(self):
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate ;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ10_2(self):
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate " \
                "and l_receiptdate >= '1993-01-01' and " \
                "l_receiptdate < '1995-01-01';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_UQ13(self):
        query = "Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where " \
                "o_orderkey = l_orderkey and " \
                "ps_partkey = l_partkey and " \
                "ps_suppkey = l_suppkey and " \
                "ps_availqty = l_linenumber and " \
                "l_shipdate >= o_orderdate and " \
                "o_orderdate >= '1990-01-01' and " \
                "l_commitdate <= l_receiptdate and " \
                "l_shipdate <= l_commitdate and " \
                "l_receiptdate > '1994-01-01' " \
                "Order By l_orderkey Limit 7;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ13_1(self):
        query = "Select l_orderkey, l_linenumber From orders, lineitem Where " \
                "o_orderkey = l_orderkey and " \
                "l_shipdate >= o_orderdate and " \
                "o_orderdate >= '1990-01-01' and " \
                "l_commitdate <= l_receiptdate and " \
                "l_shipdate <= l_commitdate and " \
                "l_receiptdate > '1994-01-01';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ13_2(self):
        query = "Select l_orderkey, l_linenumber From orders, lineitem Where " \
                "o_orderkey = l_orderkey and " \
                "l_shipdate > o_orderdate and " \
                "o_orderdate >= '1990-01-01' and " \
                "l_commitdate < l_receiptdate and " \
                "l_shipdate <= l_commitdate and " \
                "l_receiptdate > '1994-01-01';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ13_3(self):
        query = "Select l_orderkey, l_linenumber From orders, lineitem Where " \
                "o_orderkey = l_orderkey and " \
                "l_shipdate > o_orderdate and " \
                "l_shipdate >= '1991-01-01' and " \
                "l_commitdate < l_receiptdate and " \
                "l_shipdate < l_commitdate and " \
                "l_receiptdate > '1994-01-01';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ13_4(self):
        query = "Select l_orderkey, l_linenumber From orders, lineitem Where " \
                "o_orderkey = l_orderkey and " \
                "l_shipdate >= o_orderdate and " \
                "l_commitdate > '1992-03-03' and " \
                "l_commitdate < l_receiptdate and " \
                "l_shipdate <= l_commitdate and " \
                "l_receiptdate > '1994-01-01';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ10_1(self):
        query = "Select l_shipmode " \
                "From orders, lineitem " \
                "Where o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_aoa_dev_2(self):
        low_val = 1000
        high_val = 5527
        query = f"SELECT c_name as name, " \
                f"c_acctbal as account_balance " \
                f"FROM orders, customer, nation " \
                f"WHERE o_custkey > {low_val} and c_custkey = o_custkey and c_custkey <= {high_val}" \
                f"and c_nationkey = n_nationkey " \
                f"and o_orderdate between '1998-01-01' and '1998-01-15' " \
                f"and o_totalprice <= c_acctbal;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_paper_subquery1(self):
        query = "SELECT c_name as name, (c_acctbal - o_totalprice) as account_balance " \
                "FROM orders, customer, nation WHERE c_custkey = o_custkey " \
                "and c_nationkey = n_nationkey " \
                "and c_mktsegment = 'FURNITURE' " \
                "and n_name = 'INDIA' " \
                "and o_orderdate between '1998-01-01' and '1998-01-05' " \
                "and o_totalprice <= c_acctbal;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_paper_subquery2(self):
        query = "SELECT s_name as name, " \
                "(s_acctbal + o_totalprice) as account_balance " \
                "FROM supplier, lineitem, orders, nation " \
                "WHERE l_suppkey = s_suppkey " \
                "and l_orderkey = o_orderkey " \
                "and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
                "and o_orderdate between '1998-01-01' and '1998-01-05' " \
                "and o_totalprice >= s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_UQ12_subq1(self):
        query = "Select p_brand, o_clerk, l_shipmode " \
                "From orders, lineitem, part " \
                "Where l_partkey = p_partkey " \
                "and o_orderkey = l_orderkey " \
                "and l_shipdate >= o_orderdate " \
                "and o_orderdate > '1994-01-01' " \
                "and l_shipdate > '1995-01-01' " \
                "and p_retailprice >= l_extendedprice " \
                "and p_partkey < 10000 " \
                "and l_suppkey < 10000 " \
                "and p_container = 'LG CAN' " \
                "Order By o_clerk LIMIT 10;"

        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ12_subq2(self):
        query = "(Select p_brand, s_name, l_shipmode From lineitem, part, supplier  Where l_partkey = p_partkey " \
                "and p_container = 'LG CAN' and l_shipdate  >= '1995-01-02' and l_suppkey <= 13999 and l_partkey <= " \
                "14999 and l_extendedprice <= s_acctbal order by s_name Limit 10); "
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ10_subq2(self):
        query = "SELECT l_orderkey, l_shipdate FROM lineitem, " \
                "orders where l_orderkey = o_orderkey " \
                "and o_orderdate < '1994-01-01' AND l_quantity > 20   AND " \
                "l_extendedprice > 1000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ11(self):
        query = "Select o_orderpriority, " \
                "count(*) as order_count " \
                "From orders, lineitem " \
                "Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' " \
                "and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate " \
                "Group By o_orderpriority " \
                "Order By o_orderpriority;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
