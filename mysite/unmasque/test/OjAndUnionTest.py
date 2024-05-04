import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class ExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.conn.config.detect_oj = True
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_nonUnion_outerJoin(self):
        for key, value in vars(self.conn.config).items():
            print(f"{key}: {value}")
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA';"
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)

    def test_subq1(self):
        query1 = "select n_name, c_comment from nation RIGHT OUTER JOIN customer on " \
                 "c_nationkey = n_nationkey and c_acctbal < 1000;"
        eq = self.pipeline.doJob(query1)
        print(eq)
        self.assertTrue(eq is not None)
        self.assertTrue(self.pipeline.correct)

    def test_union(self):
        query = f"select n_name, r_comment FROM nation FULL OUTER JOIN region on n_regionkey = " \
                f"r_regionkey and r_name = 'AFRICA' UNION ALL " \
                f"select n_name, c_comment from nation RIGHT OUTER JOIN customer on " \
                "c_nationkey = n_nationkey and c_acctbal < 1000;"
        self.conn.config.detect_union = True
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        eq = self.pipeline.doJob(query)
        print(eq)
        self.assertTrue(self.pipeline.correct)


if __name__ == '__main__':
    unittest.main()
