import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class ExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_nonUnion_outerJoin(self):
        for key, value in vars(self.conn.config).items():
            print(f"{key}: {value}")
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_subq1(self):
        query1 = "select n_name, c_comment from nation RIGHT OUTER JOIN customer on " \
                 "c_nationkey = n_nationkey and c_acctbal < 1000;"
        eq = self.pipeline.doJob(query1)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_union(self):
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA' UNION ALL " \
                f"select n_name, c_comment from nation RIGHT OUTER JOIN customer on " \
                "c_nationkey = n_nationkey and c_acctbal < 1000;"
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_union1(self):
        query = f"select n_name, r_comment, c_acctbal FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA' LEFT OUTER JOIN customer on c_nationkey = n_nationkey " \
                 f" and c_mktsegment = 'BUILDING' UNION ALL " \
                f"select c_name, o_comment, l_discount from orders RIGHT OUTER JOIN customer on " \
                "c_custkey = o_custkey and c_acctbal < 1000 RIGHT OUTER JOIN lineitem on o_orderkey = l_orderkey " \
                 "and l_extendedprice > 7000 and o_orderstatus = 'F';"
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_union_subq2(self):
        query = f"select c_name, o_comment, l_discount from orders RIGHT OUTER JOIN customer on " \
                "c_custkey = o_custkey and c_acctbal < 1000 RIGHT OUTER JOIN lineitem on o_orderkey = l_orderkey " \
                 "and l_extendedprice > 7000 and o_orderstatus = 'F';"
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_outer_join_agg(self):
        query = "(select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, " \
     "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) " \
     "as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date " \
     "'1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus)" \
      " UNION ALL (select c_mktsegment, o_orderstatus, sum(c_acctbal) as sum_qty, sum(o_totalprice) as sum_base_price," \
      "sum(c_acctbal) as sum_disc_price, sum(o_totalprice) as sum_charge, avg(c_acctbal) as avg_qty, avg(o_totalprice) " \
     "as avg_price, avg(c_acctbal) as avg_disc, count(*) as count_order from customer, orders where c_custkey = o_custkey" \
      " and o_totalprice > 7000 group by c_mktsegment, o_orderstatus ORDER BY c_mktsegment, o_orderstatus DESC);"
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_cyclic_join(self):
        query = ("select c_name, n_name, s_name from nation LEFT OUTER JOIN customer on c_nationkey = n_nationkey"
                 " RIGHT OUTER JOIN supplier on n_nationkey = s_nationkey;")
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
