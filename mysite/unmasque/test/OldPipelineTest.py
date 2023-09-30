import sys
import unittest

sys.path.append("../../../")
from mysite.unmasque.src.core.union_from_clause import UnionFromClause
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util import tpchSettings, queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_basic_flow(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey limit 2) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey limit 2)"

        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = UnionFromClause(self.conn)
        partials = fc.get_partial_QH(query)
        self.assertEqual(partials, {'part', 'orders'})
        ftabs = fc.get_fromTabs(query)
        self.assertEqual(len(ftabs), 3)
        ctabs = fc.get_comTabs(query)
        self.assertEqual(len(ctabs), 1)
        self.conn.closeConnection()

    def test_for_nep(self):
        q_key = 'tpch_query1'
        self.conn.connectUsingParams()
        from_rels = tpchSettings.from_rels[q_key]
        query = queries.queries_dict[q_key]
        eq, _ = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                        tpchSettings.key_lists)
        self.conn.closeConnection()
        self.assertTrue(eq is not None)

    def test_for_agg(self):
        hq = "select c_mktsegment, l_orderkey, sum(l_extendedprice + l_discount + l_quantity + l_tax) as revenue, " \
             "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
             "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
             "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"
        for i in range(3):
            self.conn.connectUsingParams()
            eq = self.pipeline.doJob(hq)
            self.conn.closeConnection()
            print(eq)
        # self.assertTrue(eq is not None)

    def test_all_sample_queries(self):
        '''
        Q_keys = queries.queries_dict.keys()
        f = open("experiment_results.txt", "w")
        q_no = 1
        for q_key in Q_keys:
            self.conn.connectUsingParams()
            from_rels = tpchSettings.from_rels[q_key]
            query = queries.queries_dict[q_key]
            app = Executable(self.conn)
            result = app.doJob(query)
            if isQ_result_empty(result):
                print("Hidden query doesn't produce a populated result. It is beyond the scope of Unmasque..skipping "
                      "query!")
                continue
            f.write("\n" + str(q_no) + ":")
            f.write("\tHidden Query:\n")
            f.write(query)
            eq, _ = self.pipeline.after_from_clause_extract(query, tpchSettings.relations, from_rels,
                                                            tpchSettings.key_lists)
            f.write("\n*** Extracted Query:\n")
            if eq is None:
                f.write("Extraction Failed!")
            else:
                f.write(eq)
            f.write("\n---------------------------------------\n")
            q_no += 1
            self.conn.closeConnection()

        f.close()
        '''


if __name__ == '__main__':
    # Create a test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(MyTestCase)

    # Run the tests
    unittest.TextTestRunner(verbosity=2).run(suite)
