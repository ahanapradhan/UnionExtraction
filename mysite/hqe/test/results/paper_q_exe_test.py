import os
import time

from mysite.unmasque.src.core.executables.executable import Executable
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    conn = ConnectionHelperFactory().createConnectionHelper()
    app = Executable(conn)

    def test_paper_big(self):
        test_key = "e_paper_big.sql"
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
        self.do_testJob(query, test_key)

    def test_T5_sql(self):
        test_key = "e_T5.sql"
        query = "(select c_name,n_name from customer, nation " \
                "where c_mktsegment='BUILDING' and c_acctbal>100 " \
                "and c_nationkey = n_nationkey) UNION ALL" \
                "(select s_name, n_name from supplier, nation where s_acctbal > 4000 " \
                "and s_nationkey = n_nationkey);"
        self.do_testJob(query, test_key)

    def test_Q18_sql(self):
        test_key = "e_Q18.sql"
        query = "Select c_name, o_orderdate, o_totalprice,  sum(l_quantity) From customer, orders, lineitem       Where c_phone Like '27-_%'       and c_custkey = o_custkey       and o_orderkey = l_orderkey       Group By c_name, o_orderdate, o_totalprice       Order by o_orderdate, o_totalprice desc Limit 100;"
        self.do_testJob(query, test_key)

    def test_e_paper_bigsql(self):
        test_key = "e_paper_big.sql"
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
                and o_orderdate between date '1994-01-01' and DATE '1994-01-05'
                and o_totalprice > s_acctbal
                and o_totalprice >= 30000 and s_acctbal < 50000
				 and r_name <> 'ASIA'
                group by n_name, s_name
                order by entity_name, price 
                limit 10);
        """
        self.do_testJob(query, test_key)

    def do_testJob(self, query, test_key):
        self.conn.connectUsingParams()
        res = self.app.doJob(query)
        self.conn.closeConnection()
        with open(os.path.join("query_exe_times", test_key), 'w') as file:
            file.write(str(self.app.local_elapsed_time))
        print(len(res))
        print(self.app.local_elapsed_time)

    def test_Q6_sql(self):
        test_key = "e_Q6.sql"
        query = "Select l_shipmode, sum(l_extendedprice * l_discount) as revenue From lineitem        Where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and        l_quantity < 24 Group By l_shipmode Limit 100;"
        self.do_testJob(query, test_key)

    def test_Q4_sql(self):
        test_key = "e_Q4.sql"
        query = "Select o_orderdate, o_orderpriority, count(*) as order_count        From orders        Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month        Group By o_orderdate, o_orderpriority Order by o_orderpriority Limit 10;"
        self.do_testJob(query, test_key)

    def test_Q5_sql(self):
        test_key = "e_Q5.sql"
        query = "Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue        From customer, orders, lineitem, supplier, nation, region        Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and        c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and        r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date        '1994-01-01' + interval '1' year        Group By n_name        Order by revenue desc Limit 100;"
        self.do_testJob(query, test_key)

    def test_Q1_sql(self):
        test_key = "e_Q1.sql"
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as  sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as  avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
        self.do_testJob(query, test_key)

    def test_Q21_sql(self):
        test_key = "e_Q21.sql"
        query = "Select s_name, count(*) as numwait From supplier, lineitem l1, orders, nation         Where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and         s_nationkey = n_nationkey and n_name = 'GERMANY'         Group By s_name Order by numwait desc, s_name Limit 100;"
        self.do_testJob(query, test_key)

    def test_Q2_sql(self):
        test_key = "e_Q2.sql"
        query = ("Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment        From part, "
                 "supplier, partsupp, nation, region        Where p_partkey = ps_partkey and s_suppkey = ps_suppkey "
                 "and p_size = 38 and p_type like        '%TIN' and s_nationkey = n_nationkey and n_regionkey = "
                 "r_regionkey and r_name = 'MIDDLE EAST'        Order by s_acctbal desc, n_name, s_name, "
                 "p_partkey Limit 100;")
        self.do_testJob(query, test_key)

    def test_Q3_sql(self):
        test_key = "e_Q3.sql"
        query = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority        From customer, orders, lineitem        Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and        o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'        Group By l_orderkey, o_orderdate, o_shippriority        Order by revenue desc, o_orderdate Limit 10;"
        self.do_testJob(query, test_key)

    def test_Q11_sql(self):
        test_key = "e_Q11.sql"
        query = ("Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         "
                 "Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By "
                 "ps_COMMENT         Order by value desc Limit 100;")
        self.do_testJob(query, test_key)

    def test_Q10_sql(self):
        test_key = "e_Q10.sql"
        query = "Select c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address,         c_phone, c_comment From customer, orders, lineitem, nation         Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01'         and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey         Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20;"
        self.do_testJob(query, test_key)

    def test_Q17_sql(self):
        test_key = "e_Q17.sql"
        query = ("Select AVG(l_extendedprice) as avgTOTAL From lineitem, part         Where p_partkey = l_partkey and "
                 "p_brand = 'Brand#52' and p_container = 'LG CAN' ;")
        self.do_testJob(query, test_key)

    def test_Q16_sql(self):
        test_key = "e_Q16.sql"
        query = ("Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part               "
                 "Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and "
                 "p_size >=  4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, "
                 "p_type, p_size;")
        self.do_testJob(query, test_key)

    def test_UQ11_sql(self):
        test_key = "e_UQ11.sql"
        query = "Select o_orderpriority, count(*) as order_count From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;"
        self.do_testJob(query, test_key)

    def test_UQ6_sql(self):
        test_key = "e_UQ6.sql"
        query = "(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC);"
        self.do_testJob(query, test_key)

    def test_UQ7_sql(self):
        test_key = "e_UQ7.sql"
        query = "SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  UNION ALL  SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost < 100;"
        self.do_testJob(query, test_key)

    def test_UQ10_sql(self):
        test_key = "e_UQ10.sql"
        query = "Select l_shipmode, count(*) as count From orders, lineitem Where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice and l_extendedprice <= 70000 and o_totalprice > 60000 Group By l_shipmode Order By l_shipmode;"
        self.do_testJob(query, test_key)

    def test_UQ12_sql(self):
        test_key = "e_UQ12.sql"
        query = "(Select p_brand, o_clerk, l_shipmode From orders, lineitem, part Where l_partkey = p_partkey and " \
                "o_orderkey = l_orderkey and l_shipdate >= o_orderdate and o_orderdate > '1994-01-01' and l_shipdate " \
                "> '1995-01-01' and p_retailprice >= l_extendedprice and p_partkey < 10000 and l_suppkey < 10000 and " \
                "p_container = 'LG CAN' Order By o_clerk LIMIT 5)  UNION ALL  (Select p_brand, s_name, l_shipmode " \
                "From lineitem, part, supplier Where l_partkey = p_partkey and s_suppkey = s_suppkey and l_shipdate > " \
                "'1995-01-01' and s_acctbal >= l_extendedprice and p_partkey < 15000 and l_suppkey < 14000 and " \
                "p_container = 'LG CAN' Order By s_name LIMIT 10);"
        self.do_testJob(query, test_key)

    def test_UQ5_sql(self):
        test_key = "e_UQ5.sql"
        query = "SELECT o_orderkey, o_orderdate FROM orders, customer where o_custkey = c_custkey and c_name like " \
                "'%0001248%'  AND o_orderdate >= '1997-01-01' UNION ALL SELECT l_orderkey, l_shipdate FROM lineitem, " \
                "orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND " \
                "l_extendedprice > 1000;"
        self.do_testJob(query, test_key)

    def test_UQ4_sql(self):
        test_key = "e_UQ4.sql"
        query = "SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED " \
                "STATES' UNION ALL SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey " \
                "and n_name = 'CANADA' UNION ALL SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = " \
                "l_partkey and l_quantity > 20;"
        self.do_testJob(query, test_key)

    def test_UQ13_sql(self):
        test_key = "e_UQ13.sql"
        query = "Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where o_orderkey = l_orderkey and " \
                "ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= " \
                "o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= " \
                "l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;"
        self.do_testJob(query, test_key)

    def test_UQ1_sql(self):
        test_key = "e_UQ1.sql"
        query = "(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100) " \
                "UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and " \
                "ps_availqty > 200);"
        self.do_testJob(query, test_key)

    def test_UQ14_sql(self):
        test_key = "e_UQ14.sql"
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation Where s_suppkey = " \
                "l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' and l_receiptdate >= l_commitdate and " \
                "s_nationkey = n_nationkey Group By s_name Order By numwait desc Limit 100;"
        self.do_testJob(query, test_key)

    def test_UQ3_sql(self):
        test_key = "e_UQ3.sql"
        query = "SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  " \
                "n_name = 'UNITED STATES' UNION ALL SELECT p_partkey as key, p_name as name FROM part , " \
                "lineitem where p_partkey = l_partkey and l_quantity > 35;"
        self.do_testJob(query, test_key)

    def test_UQ2_sql(self):
        test_key = "e_UQ2.sql"
        query = "SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' UNION ALL SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT';"
        self.do_testJob(query, test_key)

    def test_UQ15_sql(self):
        test_key = "e_UQ15.sql"
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= l_receiptdate and l_receiptdate <= l_commitdate Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
        self.do_testJob(query, test_key)

    def test_UQ9_sql(self):
        test_key = "e_UQ9.sql"
        query = "SELECT c_name as name, c_acctbal as account_balance FROM orders, customer, nation WHERE c_custkey = o_custkey and c_nationkey = n_nationkey and c_mktsegment = 'FURNITURE' and n_name = 'INDIA' and o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice <= c_acctbal  UNION ALL  SELECT s_name as name, s_acctbal as account_balance FROM supplier, lineitem, orders, nation WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' and o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice >= s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal;"
        self.do_testJob(query, test_key)

    def test_UQ8_sql(self):
        test_key = "e_UQ8.sql"
        query = "(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM     customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01' GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL (SELECT     l_orderkey as order_id,     AVG(l_quantity) AS total FROM     orders, lineitem where l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     l_orderkey ORDER BY     total DESC LIMIT 10);"
        self.do_testJob(query, test_key)
