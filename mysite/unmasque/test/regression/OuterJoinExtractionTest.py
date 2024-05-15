from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class OuterJoinExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_union = False
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = True
        self.conn.config.detect_or = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_outer_join_agg(self):
        query = "(select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, " \
                "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) " \
                "as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date " \
                "'1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus)" \
                " UNION ALL (select c_mktsegment, o_orderstatus, sum(c_acctbal) as sum_qty, sum(o_totalprice) as sum_base_price," \
                "sum(c_acctbal) as sum_disc_price, sum(o_totalprice) as sum_charge, avg(c_acctbal) as avg_qty, avg(o_totalprice) " \
                "as avg_price, avg(c_acctbal) as avg_disc, count(*) as count_order from customer, orders where c_custkey = o_custkey" \
                " and o_totalprice > 7000 group by c_mktsegment, o_orderstatus ORDER BY c_mktsegment, o_orderstatus DESC);"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_cyclic_join(self):
        query = "select c_name, n_name, s_name from nation LEFT OUTER JOIN customer on c_nationkey = n_nationkey"\
                 " RIGHT OUTER JOIN supplier on n_nationkey = s_nationkey;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_nonUnion_outerJoin(self):
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_sneha_outer_join_basic(self):
        query = "Select ps_suppkey, p_name, p_type " \
                "from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size>4 " \
                "and ps_availqty>3350;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_outer_join_on_where_filters(self):
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

    # @pytest.mark.skip
    def test_outer_join_w_disjunction(self):
        query = "SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem INNER JOIN orders ON l_orderkey = o_orderkey AND o_totalprice > 50000 " \
                "AND l_shipmode IN ('MAIL', 'AIR', 'TRUCK') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber, o_shippriority desc  Limit 5;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_sumang_thesis_Q6(self):
        query = "select n_name, s_acctbal, ps_availqty  from supplier RIGHT OUTER JOIN partsupp " \
                "ON ps_suppkey=s_suppkey AND ps_supplycost < 50 RIGHT OUTER JOIN " \
                "nation on s_nationkey=n_nationkey and (n_regionkey = 1 or n_regionkey =3) ORDER " \
                "BY n_name;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_sumang_thesis_Q6_1(self):
        query = "select n_name,SUM(s_acctbal) from supplier, nation, partsupp where ps_suppkey=s_suppkey AND" \
                " ps_supplycost < 50 and s_nationkey=n_nationkey and (n_regionkey = 1 or n_regionkey =3) " \
                "group by n_name ORDER " \
                "BY n_name;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_multiple_outer_join(self):
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part INNER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join1(self):
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join2(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "RIGHT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join3(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "LEFT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_multiple_outer_join4(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "FULL OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey > 3;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_joinkey_on_projection(self):
        query = f"SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name" \
                f" from orders FULL OUTER JOIN customer" \
                f" on c_custkey = o_custkey and o_orderstatus = 'F' " \
                "group by o_custkey, o_clerk, c_name order by key limit 35;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
