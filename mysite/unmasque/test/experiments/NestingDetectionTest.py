import unittest

from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = True
        self.conn.config.detect_or = True
        self.conn.config.detect_oj = False
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)

    def test_something(self):
        query = "select c_name, c_acctbal from customer " \
                "where (select sum(o_totalprice) as total_sum " \
                "from orders where c_custkey = o_custkey " \
                "and o_orderstatus = 'O' and c_mktsegment = 'BUILDING') " \
                "< 120000 limit 5;"
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq is not None)
        print(eq)



if __name__ == '__main__':
    unittest.main()
