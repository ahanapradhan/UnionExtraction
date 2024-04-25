import pytest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class OuterJoinExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_sneha_outer_join_basic(self):
        self.conn.config.detect_or = False
        query = "Select ps_suppkey, p_name, p_type " \
                "from part LEFT outer join partsupp on p_partkey=ps_partkey and p_size>4 " \
                "and ps_availqty>3350;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_outer_join_on_where_filters(self):
        self.conn.config.detect_or = False
        query = "SELECT l_shipmode, " \
                "o_shippriority ," \
                "count(*) as low_line_count " \
                "FROM lineitem LEFT OUTER JOIN orders ON " \
                "( l_orderkey = o_orderkey AND o_totalprice > 50000 ) " \
                "WHERE l_linenumber = 4 " \
                "AND l_quantity < 30 " \
                "GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_outer_join_w_disjunction(self):
        self.conn.config.detect_or = True
        query = "SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND o_totalprice > 50000 ) " \
                "WHERE l_shipmode IN ('MAIL', 'AIR', 'TRUCK') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_outer_join_on_disjunction(self):
        self.conn.config.detect_or = True
        query = "SELECT l_linenumber, " \
                "o_shippriority ," \
                "count(*) as low_line_count " \
                "FROM lineitem LEFT OUTER JOIN orders ON " \
                "( l_orderkey = o_orderkey AND l_shipmode IN ('MAIL', 'AIR', 'TRUCK')  ) " \
                "WHERE  o_totalprice > 50000 " \
                "AND l_quantity < 30 " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join(self):
        self.conn.config.detect_or = False
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part INNER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join1(self):
        self.conn.config.detect_or = False
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join2(self):
        self.conn.config.detect_or = False
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "RIGHT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join3(self):
        self.conn.config.detect_or = False
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "LEFT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join4(self):
        self.conn.config.detect_or = False
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "FULL OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey > 3;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)


