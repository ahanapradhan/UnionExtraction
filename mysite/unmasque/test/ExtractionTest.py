import random
import unittest

import pytest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


def set_optimizer_params(is_on):
    if is_on:
        option = "on"
    else:
        option = "off"

    mergejoin_option = f"SET enable_mergejoin = {option};"
    indexscan_option = f"SET enable_indexscan = {option};"
    sort_option = f"SET enable_sort = {option};"

    return [mergejoin_option, indexscan_option, sort_option]


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_for_filter_1(self):
        lower = 11
        upper = 27
        query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(f"Where s_suppkey  >= {lower} and s_suppkey <= {upper};" in eq)
        self.assertTrue(self.pipeline.correct)

    def test_for_numeric_filter(self):
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name = 'ARGENTINA';"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_for_numeric_filter_NEP(self):
        self.conn.config.detect_nep = True
        query = "select c_mktsegment as segment from customer,nation,orders where " \
                "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
                "and n_name not LIKE 'B%';"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_for_filter(self):
        for i in range(10):
            lower = random.randint(1, 100)
            upper = random.randint(lower + 1, 200)
            query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
            eq = self.pipeline.doJob(query)
            self.assertTrue(eq is not None)
            print(eq)
            self.assertTrue(f"Where s_suppkey  >= {lower} and s_suppkey <= {upper};" in eq)
            self.assertTrue(self.pipeline.correct)

    def test_issue_2_fix(self):
        self.conn.connectUsingParams()
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
        self.conn.closeConnection()

    def test_1_mul(self):
        for i in range(10):
            self.test_extraction_tpch_q1()

    def test_extraction_tpch_q1(self):
        self.conn.connectUsingParams()
        key = 'q1'
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_in_loop(self):
        for i in range(5):
            self.test_extraction_tpch_q1_filter()

    def test_extraction_tpch_q1_simple(self):
        self.conn.connectUsingParams()
        key = 'q1_simple'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        print(eq)
        self.pipeline.time_profile.print()
        self.conn.closeConnection()

    def test_extraction_tpch_q1_filter(self):
        self.conn.connectUsingParams()
        key = 'q1_filter'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_extraction_tpch_query1(self):
        self.conn.connectUsingParams()
        key = 'tpch_query1'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_tpch_query3(self):
        self.conn.connectUsingParams()
        key = 'tpch_query3'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q1(self):
        self.conn.connectUsingParams()
        key = 'Q1'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extract_Q3_optimizer_options_off(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(False))
        key = 'Q3'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_extract_Q3_optimizer_options_on(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(True))
        key = 'Q3'
        query = queries.queries_dict[key]
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_extraction_Q3(self):
        self.conn.connectUsingParams()
        key = 'Q3'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q3_1(self):
        self.conn.connectUsingParams()
        key = 'Q3_1'
        query = queries.queries_dict[key]
        print(query)
        '''
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)
        '''
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_extraction_Q18_test1(self):
        self.conn.connectUsingParams()
        key = 'Q18_test'
        query = queries.queries_dict[key]
        print(query)
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()
        self.conn.closeConnection()

    def test_filter(self):
        lower = 10
        upper = 16
        self.conn.connectUsingParams()
        query = f"SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= {lower} and s_suppkey <= {upper};"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.pipeline.time_profile.print()
        self.conn.closeConnection()

    def test_extraction_Q4(self):
        self.conn.connectUsingParams()
        key = 'Q4'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q5(self):
        self.conn.connectUsingParams()
        key = 'Q5'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q6(self):
        self.conn.connectUsingParams()
        key = 'Q6'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q7(self):
        self.conn.connectUsingParams()
        key = 'Q7'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q11(self):
        self.conn.connectUsingParams()
        key = 'Q11'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q16(self):
        self.conn.connectUsingParams()
        key = 'Q16'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q17(self):
        self.conn.connectUsingParams()
        key = 'Q17'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q18(self):
        self.conn.connectUsingParams()
        key = 'Q18'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q21(self):
        self.conn.connectUsingParams()
        key = 'Q21'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q23_1(self):
        self.conn.connectUsingParams()
        key = 'Q23_1'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)

        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q9_simple(self):
        self.conn.connectUsingParams()
        key = 'Q9_simple'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_extraction_Q10_simple(self):
        self.conn.connectUsingParams()
        key = 'Q10_simple'
        from_rels = tpchSettings.from_rels[key]
        query = queries.queries_dict[key]
        app = Executable(self.conn)
        result = app.doJob(query)
        if isQ_result_empty(result):
            print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                  "query!")
            self.assertTrue(False)

        eq, tp = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                         tpchSettings.key_lists)
        self.assertTrue(eq is not None)
        print(eq)
        tp.print()
        self.conn.closeConnection()

    def test_for_bug(self):
        query = "select sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, " \
                "o_shippriority, l_orderkey " \
                "from customer, orders, " \
                "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
                "o_orderdate " \
                "< '1995-03-15' and l_shipdate > '1995-03-15' " \
                "group by l_orderkey, o_orderdate, o_shippriority order by revenue " \
                "desc, o_orderdate limit 10;"
        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_NEP_mukul_thesis_Q1(self):
        query = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
                "sum_base_price, " \
                "sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, " \
                "avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order " \
                "From lineitem Where l_shipdate <= date '1998-12-01' and l_extendedprice <> 44506.02 " \
                "Group by l_returnflag, l_linestatus " \
                "Order by l_returnflag, l_linestatus;"

        self.conn.config.detect_nep = True

        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        print(eq)
        self.conn.closeConnection()

    @pytest.mark.skip
    def test_Q21_mukul_thesis(self):
        self.conn.connectUsingParams()
        query = "Select s_name, count(*) as numwait From supplier, lineitem, orders, nation " \
                "Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and o_orderstatus = 'F' " \
                "and s_nationkey = n_nationkey and n_name <> 'GERMANY' Group By s_name " \
                "Order By numwait desc, s_name Limit 100;"
        self.conn.config.detect_nep = True

        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)
        print(eq)
        self.assertTrue("n_name <> 'GERMANY'" in eq)
        self.conn.closeConnection()

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
    def test_6_mul(self):
        for i in range(10):
            self.test_extraction_Q6()


if __name__ == '__main__':
    unittest.main()
