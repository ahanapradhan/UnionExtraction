from mysite.unmasque.refactored.aoa import partitions_with_min_elements, merge_equivalent_paritions, \
    stirling_second_kind
from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.test.util.BaseTestCase import BaseTestCase


class MyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.pipeline = ExtractionPipeLine(self.conn)

    def test_stirling_number(self):
        for i in range(3, 10):
            print(stirling_second_kind(i, 2))

    def test_paritions(self):
        # Example usage
        elements = [1, 2, 3, 4]

        t_all_paritions = merge_equivalent_paritions(elements)
        # Displaying the result
        for i, partition in enumerate(t_all_paritions, 1):
            print(f"Partition {i}: {partition}")

        n = stirling_second_kind(len(elements), 2)
        self.assertEqual(n, len(t_all_paritions))
