import random
import unittest
from datetime import date, timedelta

import pytest

from ...src.core.factory.PipeLineFactory import PipeLineFactory
from ..util import queries
from ..util.BaseTestCase import BaseTestCase


def generate_random_dates():
    start_date = date(1992, 3, 3)
    end_date = date(1998, 12, 5)

    # Generate two random dates
    random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    # Return dates in a tuple with the lesser value first
    dates = min(random_date1, random_date2), max(random_date1, random_date2)
    return f"\'{str(dates[0])}\'", f"\'{str(dates[1])}\'"


class ExtractionTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = True
        self.conn.config.detect_or = False
        self.conn.config.use_cs2 = False
        self.pipeline = None

    def test_ij_aoa_scalar(self):
        query = """
        SELECT s_name as entity_name, n_name as country, avg(l_extendedprice*(1 - l_discount)) as price 
                FROM supplier, lineitem, orders, nation, region
                WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
                and s_nationkey = n_nationkey and n_regionkey = r_regionkey
                and r_name <> 'ASIA'
                and o_orderdate between '1994-01-01' and DATE '1994-01-05'
                and o_totalprice > s_acctbal
                and o_totalprice >= 30000 and s_acctbal < 50000
                group by n_name, s_name, s_acctbal
                order by entity_name, price 
                limit 10;"""
        self.do_test(query)

    def test_paper_big(self):
        query = """
                (SELECT c_name as entity_name, n_name as country, o_totalprice as price
                from orders LEFT OUTER JOIN 
                customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
                o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
                where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
                group by n_name, c_name, o_totalprice
                order by price
                limit 10) 
                UNION ALL 
                (SELECT s_name as entity_name, n_name as country, avg(l_extendedprice*(1 - l_discount)) as price 
                FROM supplier, lineitem, orders, nation, region
                WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
                and s_nationkey = n_nationkey and n_regionkey = r_regionkey
                and o_orderdate between '1994-01-01' and DATE '1994-01-05'
                and o_totalprice > s_acctbal
                and o_totalprice >= 30000 and s_acctbal < 50000
                and r_name <> 'ASIA'
                group by n_name, s_name, s_acctbal
                order by entity_name, price 
                limit 10);
                """
        self.do_test(query)

    def test_union_cs2(self):
        self.conn.config.use_cs2 = True
        self.conn.config.detect_union = True
        query = """
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey and n_nationkey = s_nationkey and s_name LIKE '%008');
        """
        self.do_test(query)

    def test_oj_aoa(self):
        query = """SELECT c_name as entity_name, n_name as country, o_totalprice as price
        from orders LEFT OUTER JOIN 
        customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
        o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
        where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
        group by n_name, c_name, o_totalprice
        order by price
        limit 10;"""
        self.do_test(query)

    def test_himangshu(self):
        query = "SELECT l_shipmode, COUNT(*) FROM ORDERS, LINEITEM WHERE " \
                "O_ORDERKEY = L_ORDERKEY AND O_ORDERSTATUS = 'O' AND L_SHIPDATE >= '1998-04-03' group by l_shipmode;"
        self.do_test(query)

    def test_gopi_4june_acctbal(self):
        query = "select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders " \
                "where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and " \
                "o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name," \
                " o_clerk order by c_name, o_clerk;"
        self.do_test(query)

    def test_gnp_Q10(self):
        query = '''SELECT c_name, avg(2.24*c_acctbal + o_totalprice + 325.64) as max_balance, o_clerk 
                FROM customer, orders 
                where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
                and o_orderdate <= DATE '1995-10-23' and 
                c_acctbal > 0 and c_acctbal < 30.04
                group by c_name, o_clerk 
                order by c_name, o_clerk desc;'''
        self.do_test(query)

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
        self.do_test(query)

    def test_division_by_zero_bugfix(self):
        query = "SELECT c_name, avg(c_acctbal) as max_balance,o_clerk FROM customer, orders where " \
                "c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' " \
                "and o_orderdate <= DATE '1995-10-23' and c_acctbal > 0.1 and c_acctbal < 0.6 " \
                "group by c_name, o_clerk order by c_name, o_clerk desc;"
        self.do_test(query)

    def test_in(self):
        query = "select n_name, c_acctbal from nation, customer " \
                "WHERE n_nationkey = c_nationkey and " \
                "n_nationkey IN (1, 2, 3, 5, 4, 10) and c_acctbal < 7000 " \
                "and c_acctbal > 1000 ORDER BY c_acctbal;"
        self.do_test(query)

    def test_key_range(self):
        query = "select n_name, c_acctbal from nation LEFT OUTER JOIN customer " \
                "ON n_nationkey = c_nationkey and c_nationkey > 3 and " \
                "n_nationkey < 20 and c_nationkey != 10 and c_acctbal < 7000 LIMIT 200;"
        self.do_test(query)

    def test_no_filter_outer_join(self):
        query = "select c_name, n_name, count(*) as total from nation RIGHT OUTER JOIN customer " \
                "ON c_nationkey = n_nationkey GROUP BY c_name, n_name;"
        self.do_test(query)

    def test_no_filter_outer_join1(self):
        query = "select c_name, n_name from nation RIGHT OUTER JOIN customer " \
                "ON c_nationkey = n_nationkey GROUP BY c_name, n_name;"
        self.do_test(query)

    def test_main_cmd_query(self):
        query = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, " \
                "supplier, nation         " \
                "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = " \
                "'ARGENTINA' Group By " \
                "ps_COMMENT         Order by value desc Limit 100;"
        self.do_test(query)

    def test_unionQ(self):
        query = "(select l_partkey as key from lineitem, part " \
                "where l_partkey = p_partkey and l_extendedprice <= 905) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders " \
                "where l_orderkey = o_orderkey and o_totalprice <= " \
                "905) " \
                "union all " \
                "(select o_orderkey as key from customer, orders " \
                "where c_custkey = o_custkey and o_totalprice <= 890);"
        self.do_test(query)

    def test_unionQ_outerJoin(self):
        query = "(select l_extendedprice as price, p_comment as comment from lineitem " \
                "LEFT OUTER JOIN part on l_partkey = p_partkey and l_extendedprice <= 905) " \
                "union all " \
                "(select o_totalprice as price, l_comment as comment from lineitem " \
                "RIGHT OUTER JOIN orders on l_orderkey = o_orderkey and o_totalprice <= " \
                "905) " \
                "union all " \
                "(select c_acctbal as price, o_comment as comment from customer " \
                "FULL OUTER JOIN orders on c_custkey = o_custkey and o_totalprice <= 890);"
        self.do_test(query)

    def test_unionQuery_ui_caught_case(self):
        query = "(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and " \
                "n_name = 'UNITED STATES') UNION ALL " \
                "(SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey " \
                "and l_quantity > 35);"
        self.do_test(query)

    def test_random_nonUnion(self):
        query = "SELECT o_orderdate, SUM(l_extendedprice) AS total_price " \
                "FROM orders, lineitem where o_orderkey = l_orderkey " \
                "and o_orderdate <= '1995-01-01' GROUP BY o_orderdate " \
                "ORDER BY total_price DESC LIMIT 10;"
        self.do_test(query)

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
        self.do_test(query)

    def test_disjunction_and_outerJoin(self):
        query = f"SELECT c_name, max(c_acctbal) as max_balance,  " \
                f"o_clerk FROM customer RIGHT OUTER JOIN orders ON " \
                f"c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' " \
                f"and o_orderdate <= DATE '1995-10-23' and o_orderstatus NOT IN ('P', 'O') and c_acctbal > 20" \
                f" group by c_name, o_clerk order by c_name, o_clerk desc LIMIT 42;"
        self.do_test(query)

    def test_another_outerJoin(self):
        query = "(SELECT l_orderkey as key, avg(l_quantity) as dummy, " \
                "l_partkey as s_key FROM lineitem WHERE l_shipdate >= DATE '1994-01-01'" \
                " AND l_shipdate < DATE '1995-01-01' AND l_shipmode IN ('AIR', 'FOB', 'RAIL', 'MAIL') " \
                "AND l_quantity > 30 GROUP BY l_orderkey, l_partkey ORDER BY dummy asc LIMIT 17) UNION ALL " \
                "(SELECT ps_partkey as key, max(ps_supplycost) as dummy, " \
                "o_custkey as s_key FROM partsupp LEFT OUTER JOIN orders ON " \
                "ps_suppkey = o_custkey AND o_orderdate >= DATE '1994-01-01' " \
                "AND o_orderdate < DATE '1995-01-01' AND ps_supplycost < 100 " \
                "AND o_totalprice NOT IN (65140.40, 263187.43, 281325.05) AND ps_availqty " \
                "IN (3711, 619, 6600) " \
                "GROUP BY ps_partkey, o_custkey ORDER BY dummy LIMIT 26);"
        self.do_test(query)

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
        self.do_test(query)

    def test_UQ11(self):
        query = "Select o_orderpriority, " \
                "count(*) as order_count " \
                "From orders, lineitem " \
                "Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' " \
                "and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate " \
                "Group By o_orderpriority " \
                "Order By o_orderpriority;"
        self.do_test(query)

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
        self.do_test(query)

    def test_Q18(self):
        query = "Select c_name, o_orderdate, o_totalprice,  sum(l_quantity) " \
                "From customer, orders, lineitem Where c_phone Like '27-%' " \
                "and c_custkey = o_custkey " \
                "and o_orderkey = l_orderkey " \
                "Group By c_name, o_orderdate, o_totalprice " \
                "Order by o_orderdate, o_totalprice desc Limit 100;"
        self.do_test(query)

    def test_sumang_thesis_Q3_Q4(self):
        query = "select sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and (l_quantity =42 or l_quantity =50 or l_quantity=24) UNION ALL " \
                "(select avg(ps_supplycost) as cost from part, partsupp where p_partkey = ps_partkey " \
                "and (p_brand = 'Brand#52' or p_brand = 'Brand#12') and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE'));"
        self.do_test(query)

    def test_sumang_thesis_Q4_Q3(self):
        query = "(select avg(ps_supplycost) as cost from part, partsupp where p_partkey = ps_partkey " \
                "and (p_brand = 'Brand#52' or p_brand = 'Brand#12') and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE')) UNION ALL " \
                "select sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and (l_quantity =42 or l_quantity =50 or l_quantity=24);"
        self.do_test(query)

    def test_outer_join_w_disjunction(self):
        query = "(SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem , orders WHERE l_orderkey = o_orderkey AND o_totalprice > 50000 " \
                "AND l_shipmode IN ('MAIL', 'AIR') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber, o_shippriority desc  Limit 5)" \
                " UNION ALL " \
                "(select p_size, ps_suppkey, count(*) as low_line_count from part RIGHT OUTER JOIN partsupp on" \
                " p_partkey = ps_partkey GROUP BY p_size, ps_suppkey ORDER BY p_size desc, " \
                "ps_suppkey desc LIMIT 7);"
        self.do_test(query)

    def test_outer_join_with_key_nep(self):
        query = "select p_size, ps_suppkey, count(*) as low_line_number" \
                " from part RIGHT OUTER JOIN partsupp ON " \
                " p_partkey = ps_partkey GROUP BY p_size, ps_suppkey ORDER BY p_size desc, " \
                "ps_suppkey desc LIMIT 700;"
        self.do_test(query)

    def test_outer_join_subq2_or(self):
        query = "(select p_size, ps_suppkey, count(*) as low_line_count from part RIGHT OUTER JOIN partsupp on " \
                "p_partkey = ps_partkey and p_brand IN ('Brand#52', 'Brand#34', 'Brand#15') and p_container IN ('WRAP " \
                "BOX', 'MED BOX') GROUP BY p_size, ps_suppkey  ORDER BY ps_suppkey desc LIMIT 30);"
        self.do_test(query)

    def test_outer_join_w_disjunction_2(self):
        query = "(SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem LEFT OUTER JOIN orders ON l_orderkey = o_orderkey AND o_totalprice > 50000 " \
                "AND l_shipmode IN ('MAIL', 'AIR') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber, o_shippriority desc  Limit 5)" \
                " UNION ALL " \
                "(select p_size, ps_suppkey, count(*) as low_line_count from part RIGHT OUTER JOIN partsupp on " \
                "p_partkey = ps_partkey and p_brand IN ('Brand#52', 'Brand#34', 'Brand#15') and p_container IN ('WRAP " \
                "BOX', 'MED BOX') GROUP BY p_size, ps_suppkey  ORDER BY ps_suppkey desc LIMIT 30);"
        self.do_test(query)

    def test_outer_join_w_disjunction_1(self):
        query = "(SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem LEFT OUTER JOIN orders ON l_orderkey = o_orderkey AND o_totalprice > 50000 " \
                "AND l_shipmode IN ('MAIL', 'AIR') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber, o_shippriority desc  Limit 5);"
        self.do_test(query)

    @pytest.mark.skip
    def test_inner_union(self):
        query = "select n_name, other_name from nation LEFT OUTER JOIN " \
                "((select c_nationkey as nationkey, c_name as other_name from customer where c_acctbal < 7000) UNION " \
                "ALL (select s_nationkey as nationkey, s_name as other_name " \
                "from supplier where s_acctbal > 9000)) as t_customer_supplier " \
                "on n_nationkey = t_customer_supplier.nationkey and n_regionkey = 1;"
        self.do_test(query)

    def test_outer_join_agg(self):
        query = "(select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
                "sum_base_price, " \
                "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, " \
                "avg(l_extendedprice) " \
                "as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= " \
                "date " \
                "'1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus)" \
                "UNION ALL (select c_mktsegment, o_orderstatus, sum(c_acctbal) as sum_qty, sum(o_totalprice) as " \
                "sum_base_price," \
                "sum(c_acctbal) as sum_disc_price, sum(o_totalprice) as sum_charge, avg(c_acctbal) as avg_qty, " \
                "avg(o_totalprice) " \
                "as avg_price, avg(c_acctbal) as avg_disc, count(*) as count_order from customer, orders where c_custkey = o_custkey" \
                " and o_totalprice > 7000 group by c_mktsegment, o_orderstatus ORDER BY c_mktsegment, o_orderstatus DESC);"
        self.do_test(query)

    @pytest.mark.skip
    def test_cyclic_join(self):
        query = "select c_name, n_name, s_name from nation LEFT OUTER JOIN customer on c_nationkey = n_nationkey" \
                " RIGHT OUTER JOIN supplier on n_nationkey = s_nationkey;"
        self.do_test(query)

    def test_nonUnion_outerJoin(self):
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        self.do_test(query)

    def test_sneha_outer_join_basic(self):
        query = "Select ps_suppkey, p_name, p_type " \
                "from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size>4 " \
                "and ps_availqty>3350;"
        self.do_test(query)

    def test_outer_join_on_where_filters(self):
        query = "SELECT l_shipmode, " \
                "o_shippriority ," \
                "count(*) as low_line_count " \
                "FROM lineitem LEFT OUTER JOIN orders ON " \
                "( l_orderkey = o_orderkey AND o_totalprice > 50000 ) " \
                "WHERE l_linenumber = 4 " \
                "AND l_quantity < 30 " \
                "GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;"
        self.do_test(query)

    # @pytest.mark.skip
    def test_outer_join_w_disjunction_nonunion(self):
        query = "SELECT l_linenumber, o_shippriority , " \
                "count(*) as low_line_count  " \
                "FROM lineitem INNER JOIN orders ON l_orderkey = o_orderkey AND o_totalprice > 50000 " \
                "AND l_shipmode IN ('MAIL', 'AIR', 'TRUCK') AND l_quantity < 30  " \
                "GROUP BY l_linenumber, o_shippriority Order By l_linenumber, o_shippriority desc  Limit 5;"
        self.do_test(query)

    def test_sumang_thesis_Q6_outer(self):
        query = "select n_name, s_acctbal, ps_availqty  from supplier RIGHT OUTER JOIN partsupp " \
                "ON ps_suppkey=s_suppkey AND ps_supplycost < 50 RIGHT OUTER JOIN " \
                "nation on s_nationkey=n_nationkey and (n_regionkey = 1 or n_regionkey =3) ORDER " \
                "BY n_name;"
        self.do_test(query)

    def test_sumang_thesis_Q6_1(self):
        query = "select n_name,SUM(s_acctbal) from supplier, nation, partsupp where ps_suppkey=s_suppkey AND" \
                " ps_supplycost < 50 and s_nationkey=n_nationkey and (n_regionkey = 1 or n_regionkey =3) " \
                "group by n_name ORDER " \
                "BY n_name;"
        self.do_test(query)

    def test_copyMinimizer(self):
        query = "select n_name, c_name from customer, nation where n_nationkey = c_nationkey and c_acctbal < 5000;"
        self.do_test(query)

    def test_multiple_outer_join(self):
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part INNER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        self.do_test(query)

    def test_multiple_outer_join1(self):
        query = "SELECT p_name, s_phone, ps_supplycost " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000;"
        self.do_test(query)

    def test_multiple_outer_join2(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "RIGHT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "RIGHT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        self.do_test(query)

    def test_multiple_outer_join3(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part LEFT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "LEFT OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey = 1;"
        self.do_test(query)

    def test_multiple_outer_join4(self):
        query = "SELECT p_name, s_phone, ps_supplycost, n_name " \
                "FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7 " \
                "LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 " \
                "FULL OUTER JOIN nation on s_nationkey = n_nationkey and n_regionkey > 3;"
        self.do_test(query)

    def test_joinkey_on_projection(self):
        query = f"SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name" \
                f" from orders FULL OUTER JOIN customer" \
                f" on c_custkey = o_custkey and o_orderstatus = 'F' " \
                "group by o_custkey, o_clerk, c_name order by key limit 35;"
        self.do_test(query)

    def test_simple(self):
        query = "select c_name, n_regionkey from nation INNER JOIN customer on n_nationkey = c_nationkey and " \
                "n_name <> 'GERMANY';"
        self.do_test(query)

    def test_for_numeric_flter(self):
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name not LIKE 'B%' and o_orderdate >= DATE '1994-01-01';"
        self.do_test(query)

    def test_NEP_mukul_thesis_Q1(self):
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
                "sum_base_price, " \
                "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, " \
                "avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order " \
                "From lineitem Where l_shipdate <= date '1998-12-01' and l_extendedprice <> 44506.02 " \
                "Group by l_returnflag, l_linestatus " \
                "Order by l_returnflag, l_linestatus;"
        self.do_test(query)

    # @pytest.mark.skip
    def test_Q21_mukul_thesis(self):
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
                "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
                "and s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By s_name " \
                "Order By numwait desc, s_name Limit 100;"
        self.do_test(query)

    @pytest.mark.skip
    def test_Q21_mukul_thesis_oj(self):
        self.conn.config.limit_limit = 1500
        query = "Select s_name, n_name, l_returnflag, o_clerk, count(*) as numwait " \
                "From supplier LEFT OUTER JOIN lineitem on s_suppkey = l_suppkey " \
                "and s_acctbal < 5000 RIGHT OUTER JOIN orders" \
                " on o_orderkey = l_orderkey and  o_orderstatus = 'F'  LEFT OUTER JOIN nation " \
                "on s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By s_name, n_name, l_returnflag, o_clerk " \
                "Order By numwait desc, s_name Limit 1200;"  # it needs config limit to be set to higher value
        self.do_test(query)

    def test_Q16_sql(self):
        query = "Select p_brand, p_type, p_size, count(*) as supplier_cnt From partsupp, part               " \
                "Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and " \
                "p_size >=  4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, " \
                "p_type, p_size;"
        self.do_test(query)

    def test_Q16_sql_outer_join(self):
        query = "Select p_brand, p_type, p_size, ps_availqty, count(*) as supplier_cnt From partsupp LEFT OUTER JOIN " \
                "part on p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and " \
                "p_size >=  4 Group By p_brand, p_type, p_size, ps_availqty Order by supplier_cnt desc, p_brand, " \
                "p_type, p_size, ps_availqty desc;"
        self.do_test(query)

    def test_redundant_selfjoin(self):
        query = "SELECT p.ps_partkey, p.ps_suppkey, p.ps_availqty, p.ps_supplycost, p.ps_comment FROM partsupp AS p " \
                "JOIN (SELECT * FROM partsupp WHERE ps_supplycost < 1000) AS q ON " \
                "p.ps_partkey = q.ps_partkey;"
        self.do_test(query)

    def test_for_numeric_filter(self):
        for i in range(1):
            lower = random.randint(1, 1000)
            upper = random.randint(lower + 1, 5000)
            query = f"select c_mktsegment as segment from customer,nation,orders where " \
                    f"c_acctbal between {lower} and {upper} and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                    f"and n_name = 'ARGENTINA';"
            self.do_test(query)

    def test_for_filter(self):
        for i in range(1):
            lower = random.randint(1, 100)
            upper = random.randint(lower + 1, 200)
            query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
            self.do_test(query)

    def test_issue_2_fix(self):
        query = "select l_orderkey, " \
                "sum(l_extendedprice - l_discount + l_tax) as revenue, o_orderdate, " \
                "o_shippriority from customer, orders, lineitem " \
                "where c_mktsegment = 'BUILDING' " \
                "and c_custkey = o_custkey and l_orderkey = o_orderkey " \
                "and o_orderdate < '1995-03-15' " \
                "and l_shipdate > '1995-03-15' " \
                "group by l_orderkey, o_orderdate, o_shippriority " \
                "order by revenue desc, o_orderdate limit 10;"
        for i in range(1):
            self.do_test(query)

    def test_for_date_filter_2(self):
        for i in range(1):
            lower, upper = generate_random_dates()
            query = f"select l_returnflag, l_linestatus, " \
                    f"count(*) as count_order " \
                    f"from lineitem where l_shipdate >= date {lower} and l_shipdate < date {upper} group " \
                    f"by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 10;"
            self.do_test(query)

    def test_for_date_filter_2_union(self):
        self.conn.config.use_cs2 = False
        self.conn.config.detect_union = True

        lower, upper = generate_random_dates()
        query = f"(select l_returnflag, l_linestatus, " \
                f"count(*) as count_order " \
                f"from lineitem where l_shipdate >= date {lower} and l_shipdate < date {upper} group " \
                f"by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 10)" \
                f"UNION ALL" \
                f"( select c_mktsegment, n_name, count(*) " \
                f"from customer , nation where c_acctbal >= 8500 " \
                f"group by c_mktsegment, n_name " \
                f"order by c_mktsegment, n_name LIMIT 5));"
        self.do_test(query)

    def test_extraction_Q1(self):
        key = 'Q1'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q3(self):
        key = 'Q3'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q3_1(self):
        key = 'Q3_1'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_filter(self):
        lower = 10
        upper = 16
        query = f"SELECT count(*) as totalRows, avg(s_nationkey) as avgKey FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
        self.do_test(query)

    def test_extraction_Q4(self):
        key = 'Q4'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q5(self):
        key = 'Q5'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q6(self):
        key = 'Q6'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q7(self):
        key = 'Q7'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q11(self):
        key = 'Q11'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q16(self):
        key = 'Q16'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q17(self):
        key = 'Q17'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q18(self):
        key = 'Q18'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q21(self):
        key = 'Q21'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q23_1(self):
        key = 'Q23_1'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q9_simple(self):
        key = 'Q9_simple'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_extraction_Q10_simple(self):
        key = 'Q10_simple'
        query = queries.queries_dict[key]
        self.do_test(query)

    def test_for_bug(self):
        query = "select sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, " \
                "o_shippriority, l_orderkey " \
                "from customer, orders, " \
                "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
                "o_orderdate " \
                "< '1995-03-15' and l_shipdate > '1995-03-15' " \
                "group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
                "desc, o_orderdate limit 10;"
        self.do_test(query)

    def test_gopinath_bugfix(self):
        query = "select avg(l_tax), l_linenumber from lineitem " \
                "where l_extendedprice >= 3520.02 group by l_linenumber;"
        self.do_test(query)

    def test_gopinath_bugfix_1(self):
        query = "SELECT l_orderkey, l_shipdate FROM lineitem, orders " \
                "where l_orderkey = o_orderkey and o_orderdate < '1994-01-01' " \
                "AND l_quantity > 20 AND l_extendedprice > 1000;"
        self.do_test(query)

    def test_sumang_thesis_Q2(self):
        query = "select c_mktsegment,MAX(c_acctbal) from customer where c_nationkey IN (1, 3, 9, 15, 22) group by " \
                "c_mktsegment;"
        self.do_test(query)

    def test_one_table_duplicate_value_columns(self):
        query = "select max(l_extendedprice) from lineitem where l_linenumber IN (1, 4);"
        self.do_test(query)

    def test_sumang_thesis_Q2_1(self):
        query = "select c_mktsegment,MAX(c_acctbal) from customer where c_nationkey IN (1, 2, 5, 10) group by " \
                "c_mktsegment;"
        self.do_test(query)

    def test_sumang_thesis_Q3(self):
        query = "select l_shipmode,sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and (l_quantity =42 or l_quantity =50 or l_quantity=24) group by l_shipmode order by l_shipmode " \
                "limit 100;"
        self.do_test(query)

    # @pytest.mark.skip
    def test_sumang_thesis_Q3_nep(self):
        query = "select l_shipmode,sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1993-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and ((l_orderkey > 124 and l_orderkey < 135) or " \
                "(l_orderkey > 235 and l_orderkey < 370)) group by l_shipmode order by l_shipmode " \
                "limit 100;"
        self.do_test(query)

    @pytest.mark.skip
    def test_sumang_thesis_Q3_nep1(self):
        query = "select l_shipmode,sum(l_extendedprice) as revenue " \
                "from lineitem " \
                "where l_shipdate >= date '1993-01-01' and l_shipdate < date '1994-01-01' + interval '1' year " \
                "and ((l_orderkey > 124 and l_orderkey < 370) and " \
                "l_orderkey NOT IN (133, 134, 135)) group by l_shipmode order by l_shipmode " \
                "limit 100;"
        self.do_test(query)

    def test_sumang_thesis_Q4(self):
        query = "select AVG(l_extendedprice) as avgTOTAL from lineitem,part " \
                "where p_partkey = l_partkey and (p_brand = 'Brand#52' or p_brand = 'Brand#12') and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE');"
        self.do_test(query)

    def test_sumang_thesis_Q4_prelim(self):
        query = "select AVG(l_extendedprice) as avgTOTAL from lineitem, part " \
                "where p_partkey = l_partkey and p_brand = 'Brand#52' and " \
                "(p_container = 'LG CAN' or p_container = 'LG CASE');"
        self.do_test(query)

    def test_for_disjunction(self):
        query = f"select c_mktsegment as segment from customer,nation,orders, lineitem where " \
                f"c_acctbal between 9000 and 10000 and c_nationkey = " \
                f"n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey " \
                f"and n_name IN ('BRAZIL', 'INDIA', 'ARGENTINA') " \
                f"and l_shipdate IN (DATE '1994-12-13', DATE '1998-03-15')"
        self.do_test(query)

    def test_for_disjunction_check(self):
        query = f"select c_mktsegment as segment from customer,nation,orders, lineitem where " \
                f"c_acctbal between 9000 and 10000 and c_nationkey = " \
                f"n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey " \
                f"and (n_name = 'BRAZIL' or n_regionkey = 3)"
        self.do_test(query)

    # @pytest.mark.skip
    def test_sumang_thesis_Q6(self):
        query = f"select n_name,SUM(s_acctbal) from supplier,partsupp,nation where ps_suppkey=s_suppkey and " \
                f"s_nationkey=n_nationkey " \
                f"and (s_acctbal > 2000 or ps_supplycost < 500) group by n_name ORDER BY n_name LIMIT 10;"
        # and (n_name ='ARGENTINA' or n_regionkey =3)
        self.do_test(query)

    def test_two_neps_one_table(self):
        query = "Select l_shipmode, sum(l_extendedprice) as revenue " \
                "From lineitem " \
                "Where l_shipdate  < '1994-01-01' " \
                "and l_quantity < 24 " \
                "and l_linenumber <> 4 and l_returnflag <> 'R' " \
                "Group By l_shipmode Limit 100; "
        self.do_test(query)

    def test_mukul_thesis_Q18(self):
        query = "Select c_name, o_orderdate, o_totalprice, sum(l_quantity) From customer, orders, lineitem " \
                "Where c_phone Like '27-_%' and c_custkey = o_custkey and o_orderkey = l_orderkey and " \
                "c_name <> 'Customer#000060217'" \
                "Group By c_name, o_orderdate, o_totalprice Order by o_orderdate, o_totalprice desc Limit 100;"
        self.do_test(query)

    # @pytest.mark.skip
    def test_mukul_thesis_Q11(self):
        query = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation " \
                "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' " \
                "and ps_COMMENT not like '%regular%dependencies%' and s_acctbal <> 2177.90 " \
                "Group By ps_COMMENT " \
                "Order by value desc Limit 100;"
        self.do_test(query)

    def test_for_numeric_filter_NEP(self):
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name not LIKE 'MO%' LIMIT 40;"
        self.do_test(query)

    def setUp(self):
        super().setUp()
        del self.pipeline

    def do_test(self, query):
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        u_Q = self.pipeline.doJob(query)
        print(u_Q)
        record_file = open("extraction_result.sql", "a")
        record_file.write("\n --- START OF ONE EXTRACTION EXPERIMENT\n")
        record_file.write(" --- input query:\n ")
        record_file.write(query)
        record_file.write("\n")
        record_file.write(" --- extracted query:\n ")
        if u_Q is None:
            u_Q = '--- Extraction Failed! Nothing to show! '
        record_file.write(u_Q)
        record_file.write("\n --- END OF ONE EXTRACTION EXPERIMENT\n")
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        del factory

    def test_UQ10_1_1(self):
        query = "Select l_shipmode, o_clerk " \
                "From orders RIGHT OUTER JOIN lineitem " \
                "ON o_orderkey = l_orderkey " \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate;"
        self.do_test(query)

    def test_UQ10_1_2(self):
        query = "Select l_shipmode, o_clerk " \
                "From orders RIGHT OUTER JOIN lineitem " \
                "ON o_orderkey = l_orderkey and o_orderdate <= l_shipdate and o_orderdate >= '1991-01-01'" \
                "and l_shipdate < l_commitdate and l_commitdate < l_receiptdate " \
                "and l_receiptdate <= '1996-03-03' and l_extendedprice < 70000 and o_totalprice >= 60055;"
        self.do_test(query)

    def test_paper_subquery1(self):
        query = "SELECT c_name as name, (c_acctbal - o_totalprice) as account_balance " \
                "FROM orders, customer, nation WHERE c_custkey = o_custkey " \
                "and c_nationkey = n_nationkey " \
                "and c_mktsegment = 'FURNITURE' " \
                "and n_name = 'INDIA' " \
                "and o_orderdate between '1998-01-01' and '1998-01-05' " \
                "and o_totalprice <= c_acctbal;"
        self.do_test(query)

    def test_paper_big1(self):
        query = """
(SELECT s_name as entity_name, n_name as country, avg(l_extendedprice*(1 - l_discount)) as price
FROM supplier, lineitem, orders, nation, region
WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey and n_regionkey = r_regionkey
and s_nationkey = n_nationkey and r_name <> 'ASIA'
and o_totalprice > s_acctbal 
and o_totalprice >= 30000 and s_acctbal < 50000
 and o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
group by n_name, s_name, s_acctbal 
 order by price desc limit 10);
 """
        self.conn.config.detect_union = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_nep = True
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
