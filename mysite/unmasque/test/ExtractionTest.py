import unittest

from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

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
        for i in range(3):
            eq = self.pipeline.doJob(query)
            self.assertTrue(eq is not None)
            print(eq)
        self.conn.closeConnection()

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
        self.pipeline.time_profile.debug_print()
        self.conn.closeConnection()

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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
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
        tp.debug_print()
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
