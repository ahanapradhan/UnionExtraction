import math
import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.core.filter import round_ceil
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory
from mysite.unmasque.src.util.utils import get_mid_val


class MyTestCase(unittest.TestCase):
    def test_something(self):
        mid = get_mid_val('int', 10, 1)
        self.assertEqual(mid, 6)  # add assertion here

    def test_value_pass(self):
        val1 = -19.99
        self.assertEqual(val1, -19.99)
        #val1 = val1 * 1000
        #print(val1)
        #val1 = math.trunc(val1)
        #self.assertEqual(val1, -19990)
        #val1 = val1 / 1000
        #self.assertEqual(val1, -19.99)
        #ceil_val1 = float(round_ceil(val1, 2))
        #self.assertEqual(ceil_val1, -19.99)
        val1 = val1 * 1000
        self.assertEqual(val1, -19990)
        val1 = math.trunc(val1)
        val1 = val1 / 1000
        self.assertEqual(val1, -19.99)  #
        #val1 = val1 * 100
        #val1 = math.trunc(val1)
        #val1 = val1 / 100
        self.assertEqual(val1, -19.99)

    def test_stg2(self):
        conn = ConnectionHelperFactory().createConnectionHelper()
        conn.config.detect_nep = False
        conn.config.detect_or = False
        conn.config.detect_oj = False
        conn.config.detect_union = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(conn)

        op_int_limit_pair = [('>', '200'), ('<', '130'), ('>=', '133'), ('<=', '288')]
        op_numeric_limit_pair = [('>', '2000'), ('<', '7000'), ('>=', '1199.95'), ('<=', '9011.67')]
        for op_int_val in op_int_limit_pair:
            for op_numeric_val in op_numeric_limit_pair:
                query = f"select l_shipmode from lineitem where l_suppkey {op_int_val[0]} {op_int_val[1]} and l_extendedprice {op_numeric_val[0]} {op_numeric_val[1]};"
                print(query)
                eq = self.pipeline.doJob(query)
                print("extracted query:", eq)
                self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
