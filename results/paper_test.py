import os
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.src.pipeline.UnionPipeLine import UnionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.app = Executable(self.conn)

    def test_UQ11_sql(self):
        test_key = "e_UQ11.sql"
        self.conn.connectUsingParams()
        query = "Select o_orderpriority, count(*) as order_count From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group By o_orderpriority Order By o_orderpriority;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ6_sql(self):
        test_key = "e_UQ6.sql"
        self.conn.connectUsingParams()
        query = "(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC);"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ7_sql(self):
        test_key = "e_UQ7.sql"
        self.conn.connectUsingParams()
        query = "SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  UNION ALL  SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost < 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ10_sql(self):
        test_key = "e_UQ10.sql"
        self.conn.connectUsingParams()
        query = "Select l_shipmode, count(*) as count From orders, lineitem Where o_orderkey = l_orderkey and " \
                "l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and " \
                "l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice and l_extendedprice <= 70000 and " \
                "o_totalprice > 60000 Group By l_shipmode Order By l_shipmode;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ12_sql(self):
        test_key = "e_UQ12.sql"
        self.conn.connectUsingParams()
        query = "Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where ps_partkey = l_partkey and ps_suppkey = l_suppkey and o_orderkey = l_orderkey and l_shipdate >= o_orderdate and ps_availqty <= l_linenumber Order By l_orderkey LIMIT 10;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ5_sql(self):
        test_key = "e_UQ5.sql"
        self.conn.connectUsingParams()
        query = "SELECT o_orderkey, o_orderdate FROM orders, customer where o_custkey = c_custkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' UNION ALL SELECT l_orderkey, l_shipdate FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ4_sql(self):
        test_key = "e_UQ4.sql"
        self.conn.connectUsingParams()
        query = "SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' UNION ALL SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' UNION ALL SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ13_sql(self):
        test_key = "e_UQ13.sql"
        self.conn.connectUsingParams()
        query = "Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where o_orderkey = l_orderkey and " \
                "ps_partkey = l_partkey and ps_suppkey = l_suppkey and ps_availqty = l_linenumber and l_shipdate >= " \
                "o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <= l_receiptdate and l_shipdate <= " \
                "l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ1_sql(self):
        test_key = "e_UQ1.sql"
        self.conn.connectUsingParams()
        query = "(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100) UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and ps_availqty > 200);"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ14_sql(self):
        test_key = "e_UQ14.sql"
        self.conn.connectUsingParams()
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey = n_nationkey Group By s_name Order By numwait desc Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ3_sql(self):
        test_key = "e_UQ3.sql"
        self.conn.connectUsingParams()
        query = "SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' UNION ALL SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity > 35;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ2_sql(self):
        test_key = "e_UQ2.sql"
        self.conn.connectUsingParams()
        query = "SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' UNION ALL SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT';"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ15_sql(self):
        test_key = "e_UQ15.sql"
        self.conn.connectUsingParams()
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= l_receiptdate and l_receiptdate <= l_commitdate Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ9_sql(self):
        test_key = "e_UQ9.sql"
        self.conn.connectUsingParams()
        query = "SELECT c_name as name, c_acctbal as account_balance FROM orders, customer, nation WHERE c_custkey = o_custkey and c_nationkey = n_nationkey and c_mktsegment = 'FURNITURE' and n_name = 'INDIA' and o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice <= c_acctbal  UNION ALL  SELECT s_name as name, s_acctbal as account_balance FROM supplier, lineitem, orders, nation WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' and o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice >= s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_UQ8_sql(self):
        test_key = "e_UQ8.sql"
        self.conn.connectUsingParams()
        query = "(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM" \
                "     customer, orders where c_custkey = " \
                "o_custkey and     o_orderdate >= '1995-01-01' GROUP BY     c_custkey ORDER BY     total ASC LIMIT " \
                "10) UNION ALL (SELECT     l_orderkey as order_id,     AVG(l_quantity) AS total FROM     orders, " \
                "lineitem where l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     " \
                "l_orderkey ORDER BY     total DESC LIMIT 10);"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_union_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)



