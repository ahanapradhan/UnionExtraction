

import unittest

from mysite.unmasque.src.core.from_clause import FromClause
from ..src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from .util import queries
from .util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def test_like_tpchq1(self):
        query = queries.tpch_query1
        self.conn.connectUsingParams()
        self.assertTrue(self.conn.conn is not None)
        fc = FromClause(self.conn)
        rels = fc.doJob(query, "error")
        print("Rels", rels)
        self.assertEqual(len(rels), 1)
        self.assertTrue('lineitem' in rels)
        self.conn.closeConnection()
        self.assertEqual(8, len(fc.all_relations))
        self.assertEqual(8, fc.app_calls)

    def test_extraction_tpch_q1(self):
        self.conn.connectUsingParams()
        query = queries.tpch_query1
        self.pipeline = ExtractionPipeLine(self.conn)
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
