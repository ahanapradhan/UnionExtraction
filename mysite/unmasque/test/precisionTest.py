import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.src.util.ConnectionFactory import ConnectionHelperFactory


def make_filter_attrib_dict(entry, filter_attrib_dict):
    values = []
    single_values = []
    range_values = []
    for val in entry[3]:
        if not isinstance(val, tuple):
            single_values.append(val)
        else:
            range_values.append(val)
    single_values.sort()
    range_values.sort(key=lambda tup: tup[0])  # sorts in place
    print(single_values, range_values)
    s_counter, r_counter = 0, 0
    while s_counter < len(single_values) and r_counter < len(range_values):
        if single_values[s_counter] <= range_values[r_counter][0]:
            values.append(single_values[s_counter])
            s_counter += 1
        else:
            values.append(range_values[r_counter])
            r_counter += 1
    while s_counter < len(single_values):
        values.append(single_values[s_counter])
        s_counter += 1
    while r_counter < len(range_values):
        values.append(range_values[r_counter])
        r_counter += 1
    filter_attrib_dict[(entry[0], entry[1])] = tuple(values)


class MyTestCase(unittest.TestCase):

    def create_pipeline(self):
        self.conn = ConnectionHelperFactory().createConnectionHelper()
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_union = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_in_filter_attrib_dict(self):
        _dict = {}
        test_entry = ('tab', 'attrib', 'IN', [10, (2, 3), 5], [10, (2, 3), 5])
        make_filter_attrib_dict(test_entry, _dict)
        self.assertEqual(len(_dict.keys()), 1)

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
