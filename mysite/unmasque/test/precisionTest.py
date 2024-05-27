import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


class MyTestCase(unittest.TestCase):

    def create_pipeline(self):
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_union = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_stg2(self):
        self.create_pipeline()

        op_int_limit_pair = [('>', '200'), ('<', '130'), ('>=', '133'), ('<=', '288')]
        op_numeric_limit_pair = [('>', '2000'), ('<', '7000'), ('>=', '1199.95'), ('<=', '9011.67')]
        for op_int_val in op_int_limit_pair:
            for op_numeric_val in op_numeric_limit_pair:
                query = f"select l_shipmode from lineitem where l_suppkey {op_int_val[0]} {op_int_val[1]} and l_extendedprice {op_numeric_val[0]} {op_numeric_val[1]};"
                print(query)
                eq = self.pipeline.doJob(query)
                print("extracted query:", eq)
                self.assertTrue(self.pipeline.correct)
                # self.sanitizer.doJob()

    def test_repeat(self):
        self.create_pipeline()
        total = 20

        query = f"SELECT c_name, avg(c_acctbal) as max_balance,  o_clerk FROM customer, orders where  " \
                f"c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' " \
                f"and o_orderdate <= DATE '1995-10-23' and c_acctbal > 30.04 " \
                f"group by c_name, o_clerk order by c_name, o_clerk desc;"
        point_zero_five_counter = 0
        point_zero_six_counter = 0

        for i in range(total):
            eq = self.pipeline.doJob(query)
            print("extracted query:", eq)
            self.assertTrue(self.pipeline.correct)
            point_zero_six_counter += eq.count("30.06")
            point_zero_five_counter += eq.count("30.05")
            # self.sanitizer.doJob()

        self.assertEqual(point_zero_six_counter, 0)
        self.assertEqual(point_zero_five_counter, total)


if __name__ == '__main__':
    unittest.main()
