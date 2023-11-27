import unittest

from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.ExtractionTest import set_optimizer_params
from mysite.unmasque.test.util import queries
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase
from results.tpch_kapil_report import Q3


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_extract_Q3_optimizer_options_off(self):
        self.conn.connectUsingParams()
        self.conn.execute_sql(set_optimizer_params(False))
        key = 'Q3'
        query = queries.queries_dict[key]
        query = Q3
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)
        self.pipeline.time_profile.print()
        self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()


if __name__ == '__main__':
    unittest.main()
