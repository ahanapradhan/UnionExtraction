from mysite.unmasque.refactored.aoa import merge_equivalent_paritions
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_aoa_dev(self):
        query = "SELECT c_name as name, " \
                "c_acctbal as account_balance " \
                "FROM orders, customer, nation " \
                "WHERE c_custkey = o_custkey and c_custkey <= 5000" \
                "and c_nationkey = n_nationkey " \
                "and o_orderdate between '1998-01-01' and '1998-01-15' " \
                "and o_totalprice <= c_acctbal;"
        self.conn.connectUsingParams()
        eq = self.pipeline.doJob(query)
        self.assertTrue(eq)
        # self.assertTrue(self.pipeline.correct)
        self.conn.closeConnection()

    def test_paritions(self):
        # Example usage
        elements = [1, 2, 3, 4]

        t_all_paritions = merge_equivalent_paritions(elements)
        # Displaying the result
        for i, partition in enumerate(t_all_paritions, 1):
            print(f"Partition {i}: {partition}")

        # self.assertEqual(n, len(t_all_paritions))
