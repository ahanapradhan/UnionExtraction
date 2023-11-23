import unittest

from mysite.unmasque.test.src.validator import validate_gb, validate_ob
from results.tpch_kapil_report import Q1, Q2, Q3


class MyTestCase(unittest.TestCase):
    def test_check(self):
        q_h = Q1
        q_e = "Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, " \
              "Sum(l_extendedprice*(1 - l_discount)) as sum_disc_price, Sum(l_extendedprice*(-l_discount*l_tax - " \
              "l_discount + l_tax + 1)) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, " \
              "Avg(l_discount) as avg_disc, Count(*) as count_order " \
              "From lineitem where l_shipdate  <= '1998-09-22' Group By l_returnflag, l_linestatus Order By " \
              "l_returnflag asc, l_linestatus asc;"
        self.assertTrue(validate_gb(q_h, q_e))
        self.assertTrue(validate_ob(q_h, q_e))

    def test_check_2(self):
        q_h = Q2
        q_e = "Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " \
              "From nation, part, partsupp, region, supplier " \
              "Where p_partkey = ps_partkey and ps_suppkey = s_suppkey and n_nationkey = s_nationkey and n_regionkey " \
              "= r_regionkey and r_name  = 'MIDDLE EAST' and p_type LIKE '%TIN' and p_size = 38 " \
              "Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc Limit 100;"
        self.assertTrue(validate_gb(q_h, q_e))
        self.assertTrue(validate_ob(q_h, q_e))

    def test_check_3(self):
        q_h = Q3
        q_e = "Select o_orderkey as l_orderkey, Sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, " \
              "o_shippriority " \
              "From customer, lineitem, orders " \
              "Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  <= '1995-03-13' and " \
              "c_mktsegment  = 'BUILDING' and l_shipdate  >= '1995-03-17' " \
              "Group By o_orderkey, o_orderdate, o_shippriority " \
              "Order By revenue asc, o_orderdate asc, l_orderkey asc, o_shippriority asc Limit 10;"
        self.assertTrue(validate_gb(q_h, q_e))
        self.assertFalse(validate_ob(q_h, q_e))


