import unittest

from mysite.unmasque.refactored.result_comparator import ResultComparator
from mysite.unmasque.test.util import queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_all_same_hash_match_takes_less_time(self):
        hash_times = {}
        comparison_times = {}
        loop = 3
        for r in range(loop):
            self.run_for_one_round(comparison_times, hash_times)

        for key in hash_times.keys():
            hash_times[key] = hash_times[key] / loop
        for key in comparison_times.keys():
            comparison_times[key] = comparison_times[key] / loop

        hash_takes_less = 0
        for key in hash_times.keys():
            if hash_times[key] < comparison_times[key]:
                hash_takes_less += 1
        self.assertTrue(hash_takes_less > loop / 2)  # for more than 50% cases, hash comparison is faster

    def run_for_one_round(self, comparison_times, hash_times):
        for key in queries.queries_dict.keys():
            print(key)
            self.conn.connectUsingParams()
            query = queries.queries_dict[key]
            eq = query

            matched_hash, th = self.check_hash_matching(eq, query)
            if key not in hash_times.keys():
                hash_times[key] = th
            else:
                hash_times[key] = hash_times[key] + th

            matched_compare, tc = self.check_comparison_matching(eq, query)
            if key not in comparison_times.keys():
                comparison_times[key] = tc
            else:
                comparison_times[key] = comparison_times[key] + tc

            self.assertEqual(matched_hash, matched_compare)
            print(th, " , ", tc)
            self.conn.closeConnection()
            print("...done")

    def check_comparison_matching(self, eq, query):
        rc_compare = ResultComparator(self.conn, False)
        matched_compare = rc_compare.doJob(query, eq)
        print("Comparison Matching:", matched_compare)
        return matched_compare, rc_compare.local_elapsed_time

    def check_hash_matching(self, eq, query):
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(query, eq)
        print("Hash Matching:", matched_hash)
        self.assertTrue(matched_hash)
        return matched_hash, rc_hash.local_elapsed_time

    def test_restore_tables(self):
        rc_hash = ResultComparator(self.conn, True)
        self.conn.connectUsingParams()
        rc_hash.sanitize()
        res, desc = self.conn.execute_sql_fetchall(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = '" + self.conn.config.schema + "' and TABLE_CATALOG= '" + self.conn.db + "';")
        self.assertEqual(res[0][0], 8)
        self.conn.closeConnection()

    def test_for_some_query(self):
        q = "SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100;"
        self.conn.connectUsingParams()
        rc_hash = ResultComparator(self.conn, False)
        matched_hash = rc_hash.doJob(q, q)
        self.assertTrue(matched_hash)
        self.conn.closeConnection()

    def test_Q(self):
        q = "Select l_shipmode, sum(l_extendedprice) as revenue " \
            "From lineitem " \
            "Where l_shipdate >= '1994-01-01' " \
            "and l_quantity <= 23.0 " \
            "Group By l_shipmode Limit 100; "
        self.conn.connectUsingParams()
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(q, q)
        self.assertTrue(matched_hash)
        self.conn.closeConnection()
        self.conn.connectUsingParams()
        rc_diff = ResultComparator(self.conn, False)
        matched_diff = rc_diff.doJob(q, q)
        self.assertTrue(matched_diff)
        self.conn.closeConnection()

    def test_Q_groupby_change(self):
        q = "Select l_shipmode, sum(l_extendedprice) as revenue, l_returnflag " \
            "From lineitem " \
            "Where l_shipdate >= '1994-01-01' " \
            "and l_quantity <= 23.0 " \
            "Group By l_shipmode, l_returnflag; "
        eq = "Select l_shipmode, sum(l_extendedprice) as revenue, l_returnflag " \
             "From lineitem " \
             "Where l_shipdate >= '1994-01-01' " \
             "and l_quantity <= 23.0 " \
             "Group By l_returnflag, l_shipmode; "
        self.conn.connectUsingParams()
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(q, eq)
        self.assertTrue(matched_hash)
        self.conn.closeConnection()
        self.conn.connectUsingParams()
        rc_diff = ResultComparator(self.conn, False)
        matched_diff = rc_diff.doJob(q, eq)
        self.assertTrue(matched_diff)
        self.conn.closeConnection()

    def test_Q_nep(self):
        q = "Select l_shipmode, sum(l_extendedprice) as revenue " \
            "From lineitem " \
            "Where l_shipdate >= '1994-01-01' " \
            "and l_quantity <= 23.0 " \
            "Group By l_shipmode Limit 100; "
        eq = "Select l_shipmode, sum(l_extendedprice) as revenue " \
             "From lineitem " \
             "Where l_shipdate >= '1994-01-01' " \
             "and l_quantity <= 23.0 and l_linenumber <> 4" \
             "Group By l_shipmode Limit 100; "
        self.conn.connectUsingParams()
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(q, eq)
        self.assertFalse(matched_hash)
        self.conn.closeConnection()
        self.conn.connectUsingParams()
        rc_diff = ResultComparator(self.conn, False)
        matched_diff = rc_diff.doJob(q, eq)
        self.assertFalse(matched_diff)
        self.conn.closeConnection()

    def test_create_table_from_qh(self):
        q_h = "select c_mktsegment as segment from customer,nation,orders where " \
              "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
              "and n_name = 'ARGENTINA';"
        self.conn.connectUsingParams()
        self.conn.execute_sql(["drop view if exists r_e;", f"create view r_e as {q_h};"])
        rc_hash = ResultComparator(self.conn, True)
        try:
            rc_hash.create_table_from_Qh(q_h)
        except Exception:
            self.assertFalse(True)
            self.conn.execute_sql(["drop view if exists r_e;"])
        self.assertTrue(True)
        self.conn.execute_sql(["drop view if exists r_e;"])

    def test_for_numeric_filter(self):
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name = 'ARGENTINA';"
        eq = "select c_mktsegment as segment from customer,nation,orders where " \
             "c_acctbal >= 999.996 and c_acctbal <= 5000.004 and c_nationkey = n_nationkey and c_custkey = " \
             "o_custkey " \
             "and n_name = 'ARGENTINA';"
        self.conn.connectUsingParams()
        rc_hash = ResultComparator(self.conn, True)
        matched_hash = rc_hash.doJob(query, eq)
        self.assertTrue(matched_hash)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
