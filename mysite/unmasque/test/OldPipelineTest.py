import unittest

from mysite.unmasque.refactored.ConnectionHelper import ConnectionHelper
from mysite.unmasque.refactored.executable import Executable
from mysite.unmasque.refactored.util.utils import isQ_result_empty
from mysite.unmasque.src.core import OldPipeLine
from mysite.unmasque.src.core.UN1_from_clause import UN1FromClause
from mysite.unmasque.test import tpchSettings, queries


class MyTestCase(unittest.TestCase):
    conn = ConnectionHelper("tpch", "postgres", "postgres", "5432", "localhost")

    def test_basic_flow(self):
        query = "(select l_partkey as key from lineitem, part where l_partkey = p_partkey limit 2) " \
                "union all " \
                "(select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey limit 2)"

        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = UN1FromClause(self.conn)
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
        eq = OldPipeLine.extract(self.conn, query,
                                 tpchSettings.relations,
                                 from_rels,
                                 tpchSettings.key_lists,
                                 tpchSettings.global_pk_dict)
        self.conn.closeConnection()
        self.assertTrue(eq is not None)

    def test_all_sample_queries(self):
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
            eq = OldPipeLine.extract(self.conn, query,
                                     tpchSettings.relations,
                                     from_rels,
                                     tpchSettings.key_lists,
                                     tpchSettings.global_pk_dict)
            f.write("\n*** Extracted Query:\n")
            if eq is None:
                f.write("Extraction Failed!")
            else:
                f.write(eq)
            f.write("\n---------------------------------------\n")
            q_no += 1
            self.conn.closeConnection()

        f.close()


if __name__ == '__main__':
    unittest.main()
