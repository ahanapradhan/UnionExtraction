import random
import unittest
from datetime import timedelta, date

import pytest

from mysite.unmasque.src.core.executable import Executable
from ...src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from ...test.src.Optimizer_config import set_optimizer_params
from ...test.util import tpchSettings, queries
from ...test.util.BaseTestCase import BaseTestCase
from ...test.util.queries import Q3


def generate_random_dates():
    start_date = date(1992, 3, 3)
    end_date = date(1998, 12, 5)

    # Generate two random dates
    random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    # Return dates in a tuple with the lesser value first
    dates = min(random_date1, random_date2), max(random_date1, random_date2)
    return f"\'{str(dates[0])}\'", f"\'{str(dates[1])}\'"


class ExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_for_filter_1(self):
        lower = 11
        upper = 27
        query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_for_numeric_filter(self):
        for i in range(1):
            lower = random.randint(1, 1000)
            upper = random.randint(lower + 1, 5000)
            query = f"select c_mktsegment as segment from customer,nation,orders where " \
                    f"c_acctbal between {lower} and {upper} and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                    f"and n_name = 'ARGENTINA';"
            eq = self.pipeline.doJob(query)
            self.assertTrue(eq is not None)
            print(eq)
            self.assertTrue(self.pipeline.correct)

    def test_for_filter(self):
        for i in range(1):
            lower = random.randint(1, 100)
            upper = random.randint(lower + 1, 200)
            query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
            eq = self.pipeline.doJob(query)
            self.assertTrue(eq is not None)
            print(eq)
            self.assertTrue(self.pipeline.correct)

    def test_issue_2_fix(self):
        query = "select l_orderkey, " \
                "sum(l_extendedprice - l_discount + l_tax) as revenue, o_orderdate, " \
                "o_shippriority from customer, orders, lineitem " \
                "where c_mktsegment = 'BUILDING' " \
                "and c_custkey = o_custkey and l_orderkey = o_orderkey " \
                "and o_orderdate < '1995-03-15' " \
                "and l_shipdate > '1995-03-15' " \
                "group by l_orderkey, o_orderdate, o_shippriority " \
                "order by revenue desc, o_orderdate limit 10;"
        for i in range(1):
            eq = self.pipeline.doJob(query)
            self.assertTrue(eq is not None)
            print(eq)
            self.assertTrue(self.pipeline.correct)

    def test_extraction_tpch_q1(self):
        key = 'q1'
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def test_extraction_tpch_q1_simple(self):
        key = 'q1_simple'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        print(eq)
        self.pipeline.time_profile.print()

    def test_for_date_filter(self):
        for i in range(1):
            key = 'q1_filter'
            query = queries.queries_dict[key]
            eq = self.pipeline.doJob(query)
            print(eq)
            self.pipeline.time_profile.print()
            self.assertTrue(self.pipeline.correct)

    def test_for_date_filter_2(self):
        for i in range(1):
            lower, upper = generate_random_dates()
            q1_filter = f"select l_returnflag, l_linestatus, " \
                        f"count(*) as count_order " \
                        f"from lineitem where l_shipdate >= date {lower} and l_shipdate < date {upper} group " \
                        f"by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 10;"
            # q1_filter = f"select l_returnflag, l_linestatus, " \
            #            f"count(*) as count_order " \
            #            f"from lineitem where l_shipdate < date {upper} group " \
            #            f"by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 10;"

            eq = self.pipeline.doJob(q1_filter)
            print(q1_filter)
            print(eq)
            self.assertTrue(self.pipeline.correct)

    def test_extraction_tpch_q1_filter(self):
        key = 'q1_filter'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    def test_extraction_tpch_query3(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'tpch_query3'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q1(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q1'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    @pytest.mark.skip
    def test_extract_Q3_optimizer_options_off(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(False))
        query = Q3
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_extract_Q3_optimizer_options_on(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(True))
        query = Q3
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_extraction_Q3(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q3'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q3_1(self):
        key = 'Q3_1'
        query = queries.queries_dict[key]
        print(query)
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)

    # @pytest.mark.skip
    def test_extraction_Q18_test1(self):
        key = 'Q18_test'
        query = queries.queries_dict[key]
        print(query)
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_filter(self):
        lower = 10
        upper = 16
        query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()

    def test_extraction_Q4(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q4'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q5(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q5'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_full_Q5(self):
        self.conn.connectUsingParams()
        key = 'Q5'
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq = self.pipeline.extract(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.conn.closeConnection()

    def test_extraction_Q6(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q6'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q7(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q7'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q11(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q11'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q16(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q16'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q17(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q17'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q18(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q18'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q21(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q21'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q23_1(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q23_1'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q9_simple(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q9_simple'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_extraction_Q10_simple(self):
        self.pipeline.connectionHelper.connectUsingParams()
        key = 'Q10_simple'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if app.isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, from_rels)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.pipeline.connectionHelper.closeConnection()

    def test_for_bug(self):
        query = "select sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, " \
                "o_shippriority, l_orderkey " \
                "from customer, orders, " \
                "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
                "o_orderdate " \
                "< '1995-03-15' and l_shipdate > '1995-03-15' " \
                "group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
                "desc, o_orderdate limit 10;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_Q21(self):  # enable it after fixing order by
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
                "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
                "and s_nationkey = n_nationkey Group By s_name " \
                "Order By numwait desc, s_name Limit 100;"
        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_correlated_nested_query(self):
        query = "select c_name from customer where c_acctbal <= (select MIN(s_acctbal) from supplier);"

        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        '''
        query = "select l_orderkey, l_extendedprice from lineitem " \
                "where l_extendedprice >= (select max(l_discount) from lineitem);"
        eq = Select l_orderkey, l_extendedprice From lineitem Where l_discount <= l_extendedprice; 
        '''
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_extreme(self):
        query = "select * from part where p_size + 1 <= 10;"
        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_diff_res(self):
        query = "SELECT * FROM lineitem, orders " \
                "WHERE l_quantity < 1000 " \
                "AND l_tax < 1000 " \
                "AND o_orderkey=l_orderkey  " \
                "ORDER BY l_tax LIMIT 10;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    @pytest.mark.skip
    def test_extreme_1(self):
        self.conn.connectUsingParams()
        query = "select count(*) from part where p_size >= -2147483647;"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
