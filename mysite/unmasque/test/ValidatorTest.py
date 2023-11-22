import unittest

from mysite.unmasque.test.src.validator import validate_gb_ob_attributes
from results.tpch_kapil_report import Q1


class MyTestCase(unittest.TestCase):
    def test_check(self):
        q_h = Q1
        q_e = "Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, " \
              "Sum(l_extendedprice*(1 - l_discount)) as sum_disc_price, Sum(l_extendedprice*(-l_discount*l_tax - " \
              "l_discount + l_tax + 1)) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, " \
              "Avg(l_discount) as avg_disc, Count(*) as count_order " \
              "From lineitem where l_shipdate  <= '1998-09-22' Group By l_returnflag, l_linestatus Order By " \
              "l_returnflag asc, l_linestatus asc;"
        self.assertTrue(validate_gb_ob_attributes(q_h, q_e))
