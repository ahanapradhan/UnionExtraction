from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class OuterJoinExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_sneha_outer_join_basic(self):
        query = "Select ps_suppkey, p_name, p_type " \
                "from part LEFT outer join partsupp on p_partkey=ps_partkey and p_size>4 " \
                "and ps_availqty>3350;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_outer_join_on_where_filters(self):
        self.conn.config.detect_or = True
        query = "SELECT l_shipmode, " \
                "o_shippriority ," \
                "count(o_orderpriority) as low_line_count " \
                "FROM lineitem LEFT OUTER JOIN orders ON " \
                "( l_orderkey = o_orderkey AND o_totalprice > 50000 ) " \
                "WHERE l_linenumber = 4 " \
                "AND l_quantity < 30 " \
                "GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_outer_join_w_disjunction(self):
        self.conn.config.detect_or = True
        query = "SELECT l_linenumber, " \
                "o_shippriority ," \
                "count(o_orderpriority) as low_line_count " \
                "FROM lineitem LEFT OUTER JOIN orders ON " \
                "( l_orderkey = o_orderkey AND o_totalprice > 50000 ) " \
                "WHERE l_shipmode IN ('MAIL', 'AIR', 'TRUCK') " \
                "AND l_quantity < 30 " \
                "GROUP BY l_linenumber, o_shippriority Order By l_shipmode Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join(self):
        self.conn.config.detect_or = True
        query = "Select ps_suppkey, " \
                "p_name, p_type , l_quantity, " \
                "from lineitem " \
                "LEFT outer join partsupp on ps_suppkey=l_suppkey " \
                "right outer join part on p_partkey=ps_partkey " \
                "and p_size > 4 and ps_availqty > 3350 " \
                "WHERE l_shipmode IN ('MAIL', 'SHIP', 'TRUCK') " \
                "AND l_returnflag = 'R' ; "
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

