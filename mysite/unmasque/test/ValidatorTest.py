import unittest

import pytest

from mysite.unmasque.test.src.validator import validate_gb, validate_ob, same_hidden_ob, count_ob, Ob_suffix, \
    pretty_print
from results.tpch_kapil_report import Q1, Q2, Q3, Q21


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
        ob_dict = validate_ob(q_h, q_e)
        self.assertTrue(ob_dict[same_hidden_ob])
        self.assertFalse(ob_dict[count_ob])
        self.assertFalse(ob_dict[Ob_suffix])

    def test_check_2(self):
        q_h = Q2
        q_e = "Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " \
              "From nation, part, partsupp, region, supplier " \
              "Where p_partkey = ps_partkey and ps_suppkey = s_suppkey and n_nationkey = s_nationkey and n_regionkey " \
              "= r_regionkey and r_name  = 'MIDDLE EAST' and p_type LIKE '%TIN' and p_size = 38 " \
              "Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc Limit 100;"
        self.assertTrue(validate_gb(q_h, q_e))
        ob_dict = validate_ob(q_h, q_e)
        self.assertTrue(ob_dict[same_hidden_ob])
        self.assertFalse(ob_dict[count_ob])
        self.assertFalse(ob_dict[Ob_suffix])

    def test_check_3(self):
        q_h = Q3
        q_e = "Select o_orderkey as l_orderkey, Sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, " \
              "o_shippriority " \
              "From customer, lineitem, orders " \
              "Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  <= '1995-03-13' and " \
              "c_mktsegment  = 'BUILDING' and l_shipdate  >= '1995-03-17' " \
              "Group By o_orderkey, o_orderdate, o_shippriority " \
              "Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc Limit 10;"
        self.assertTrue(validate_gb(q_h, q_e))
        ob_dict = validate_ob(q_h, q_e)
        self.assertTrue(ob_dict[same_hidden_ob])
        self.assertFalse(ob_dict[count_ob])
        self.assertTrue(ob_dict[Ob_suffix])

    def test_check_21(self):
        q_h = Q21
        q_e = "Select s_name, Count(*) as numwait " \
              "From lineitem, nation, orders, supplier " \
              "Where l_suppkey = s_suppkey and n_nationkey = s_nationkey and l_orderkey = o_orderkey and " \
              "o_orderstatus  = 'F' and n_name  = 'GERMANY' " \
              "Group By s_name Order By s_name asc Limit 100;"
        self.assertTrue(validate_gb(q_h, q_e))
        ob_dict = validate_ob(q_h, q_e)
        self.assertTrue(ob_dict[same_hidden_ob])
        self.assertTrue(ob_dict[count_ob])
        self.assertFalse(ob_dict[Ob_suffix])

    def test_pretty_print(self):
        print("\nQno\t\t\t\tGb_correct?\n")
        print(pretty_print("Q1", True))
        print(pretty_print("Q2", False))
        print(pretty_print("Q11", True))
        print(pretty_print("Q16_nep", False))
        print(pretty_print("Q16_nep_2", True))

    @pytest.mark.skip
    def test_validator_q16(self):
        q_h = "Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part " \
              "Where p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type Like 'SMALL PLATED%' and p_size >= 4 " \
              "Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;"

        q_e = "Select p_brand, p_type, p_size, Count(*) as supplier_cnt " \
              "From part, partsupp " \
              "Where p_partkey = ps_partkey and p_brand  = 'Brand#45' and p_type LIKE 'SMALL PLATED%' and p_size  >= 4 " \
              "Group By p_type, p_size, p_brand Order By supplier_cnt desc, p_type asc, p_size asc; "

        check = validate_ob(q_h, q_e)
        self.assertTrue(check)
        print(check)
        self.assertTrue(check[same_hidden_ob])
