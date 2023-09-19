import unittest

from mysite.unmasque.refactored.result_comparator import ResultComparator
from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper
from mysite.unmasque.test.util import queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper()

    def test_all_same_hash_match_takes_less_time(self):
        for key in queries.queries_dict.keys():
            print(key)
            self.conn.connectUsingParams()
            query = queries.queries_dict[key]
            eq = query

            matched_hash, th = self.check_hash_matching(eq, query)

            matched_compare, tc = self.check_comparison_matching(eq, query)

            self.assertEqual(matched_hash, matched_compare)
            print(th, " , ", tc)
            self.conn.closeConnection()
            print("...done")

    def check_comparison_matching(self, eq, query):
        rc_compare = ResultComparator(self.conn, True)
        matched_compare = rc_compare.doJob(query, eq)
        print("Comparison Matching:", matched_compare)
        return matched_compare, rc_compare.local_elapsed_time

    def check_hash_matching(self, eq, query):
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(query, eq)
        print("Hash Matching:", matched_hash)
        self.assertTrue(matched_hash)
        return matched_hash, rc_hash.local_elapsed_time


if __name__ == '__main__':
    unittest.main()
