import os
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.src.pipeline.UnionPipeLine import UnionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.app = Executable(self.conn)

    def test_Q18_sql(self):
        test_key = "e_Q18.sql"
        self.conn.connectUsingParams()
        query = "Select c_name, o_orderdate, o_totalprice,  sum(l_quantity) From customer, orders, lineitem       " \
                "Where c_phone Like '27-_%'       and c_custkey = o_custkey       and o_orderkey = l_orderkey       " \
                "Group By c_name, o_orderdate, o_totalprice       Order by o_orderdate, o_totalprice desc Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q6_sql(self):
        test_key = "e_Q6.sql"
        self.conn.connectUsingParams()
        query = "Select l_shipmode, sum(l_extendedprice * l_discount) as revenue From lineitem        Where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and        l_quantity < 24 Group By l_shipmode Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q4_sql(self):
        test_key = "e_Q4.sql"
        self.conn.connectUsingParams()
        query = "Select o_orderdate, o_orderpriority, count(*) as order_count        From orders        Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month        Group By o_orderdate, o_orderpriority Order by o_orderpriority Limit 10;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q5_sql(self):
        test_key = "e_Q5.sql"
        self.conn.connectUsingParams()
        query = "Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue        From customer, orders, lineitem, supplier, nation, region        Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and        c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and        r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date        '1994-01-01' + interval '1' year        Group By n_name        Order by revenue desc Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q1_sql(self):
        test_key = "e_Q1.sql"
        self.conn.connectUsingParams()
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as  sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as  avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q21_sql(self):
        test_key = "e_Q21.sql"
        self.conn.connectUsingParams()
        query = "Select s_name, count(*) as numwait From supplier, lineitem l1, orders, nation         Where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and         s_nationkey = n_nationkey and n_name = 'GERMANY'         Group By s_name Order by numwait desc, s_name Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q2_sql(self):
        test_key = "e_Q2.sql"
        self.conn.connectUsingParams()
        query = "Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment        From part, supplier, partsupp, nation, region        Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like        '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST'        Order by s_acctbal desc, n_name, s_name, p_partkey Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q3_sql(self):
        test_key = "e_Q3.sql"
        self.conn.connectUsingParams()
        query = "Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority        From customer, orders, lineitem        Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and        o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'        Group By l_orderkey, o_orderdate, o_shippriority        Order by revenue desc, o_orderdate Limit 10;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q11_sql(self):
        test_key = "e_Q11.sql"
        self.conn.connectUsingParams()
        query = "Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q10_sql(self):
        test_key = "e_Q10.sql"
        self.conn.connectUsingParams()
        query = "Select c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address,         c_phone, c_comment From customer, orders, lineitem, nation         Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01'         and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey         Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q17_sql(self):
        test_key = "e_Q17.sql"
        self.conn.connectUsingParams()
        query = "Select AVG(l_extendedprice) as avgTOTAL From lineitem, part         Where p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN' ;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

    def test_Q16_sql(self):
        test_key = "e_Q16.sql"
        self.conn.connectUsingParams()
        query = "Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part               Where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type NOT Like 'SMALL PLATED%' and p_size >=               4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;"
        self.pipeline = UnionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.conn.closeConnection()
        with open(os.path.join("extracted_tech_report_queries", test_key), 'w') as file:
            file.write(eq)
        self.assertTrue(self.pipeline.correct)

