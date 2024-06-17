import unittest

from mysite.unmasque.src.core.executables.executable import Executable
from mysite.unmasque.src.pipeline.UnionPipeLine import UnionPipeLine
from mysite.unmasque.test.results.UN1TestAndPlot import TpchExtractionPipelineTestCase


class TpchUnionPipelineTestCase(TpchExtractionPipelineTestCase):
    def __init__(self, *args, **kwargs):
        super(TpchExtractionPipelineTestCase, self).__init__(*args, **kwargs)
        self.app = Executable(self.conn)
        self.extracted_U = "tpch_queries"
        self.dat_filename = "tpch_queries.dat"
        self.plot_script = "tpch_queries.gnu"
        self.plot_filename = "tpch_queries_plot.eps"
        self.latex_filename = "tpch_queries_table.tex"
        self.summary_filename = "tpch_extraction_summary.txt"
        self.gb_correct = False
        self.ob_correct = False
        self.result_correct = False

    def create_pipeline(self):
        self.pipeline = UnionPipeLine(self.conn)


if __name__ == '__main__':
    # Create a test suite for the ChildTest class only
    suite = unittest.TestLoader().loadTestsFromTestCase(TpchUnionPipelineTestCase)

    # Run the tests
    unittest.TextTestRunner().run(suite)
