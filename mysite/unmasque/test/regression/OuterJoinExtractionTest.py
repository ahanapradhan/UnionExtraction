from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class OuterJoinExtractionTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.conn.config.detect_nep = False
        self.conn.config.detect_or = False
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_sneha_outer_join1(self):
        query = "Select ps_suppkey, l_suppkey, p_partkey,ps_partkey, l_quantity, ps_availqty, p_size " \
                "from part LEFT outer join partsupp on p_partkey=ps_partkey and p_size>4 " \
                "and ps_availqty>3350 RIGHT outer join lineitem on ps_suppkey=l_suppkey and l_quantity>10;"
        eq = self.pipeline.doJob(query)
        # self.assertTrue(eq is not None)
        print(eq)
        # self.assertTrue(self.pipeline.correct)
