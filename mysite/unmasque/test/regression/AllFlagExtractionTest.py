import random
import unittest
from datetime import date, timedelta

import pytest

from mysite.gpt.tpcds_benchmark_queries import Q4_CTE, Q2_subquery, Q5_CTE, Q71_subquery, Q11_CTE, Q74_subquery, \
    Q54_subquery
from ...src.core.factory.PipeLineFactory import PipeLineFactory
from ..util import queries
from ..util.BaseTestCase import BaseTestCase


def generate_random_dates():
    start_date = date(1992, 3, 3)
    end_date = date(1998, 12, 5)

    # Generate two random dates
    random_date1 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    random_date2 = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

    # Return dates in a tuple with the lesser value first
    dates = min(random_date1, random_date2), max(random_date1, random_date2)
    return f"\'{str(dates[0])}\'", f"\'{str(dates[1])}\'"


class ExtractionTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn.config.detect_union = True
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_or = False
        self.conn.config.use_cs2 = False
        self.pipeline = None

    def setUp(self):
        super().setUp()
        del self.pipeline

    def do_test(self, query):
        factory = PipeLineFactory()
        self.pipeline = factory.create_pipeline(self.conn)
        u_Q = self.pipeline.doJob(query)
        print(u_Q)
        record_file = open("extraction_result.sql", "a")
        record_file.write("\n --- START OF ONE EXTRACTION EXPERIMENT\n")
        record_file.write(" --- input query:\n ")
        record_file.write(query)
        record_file.write("\n")
        record_file.write(" --- extracted query:\n ")
        if u_Q is None:
            u_Q = '--- Extraction Failed! Nothing to show! '
        record_file.write(u_Q)
        record_file.write("\n --- END OF ONE EXTRACTION EXPERIMENT\n")
        self.pipeline.time_profile.print()
        #self.assertTrue(self.pipeline.correct)
        del factory

    def test_Q4(self):
        query = Q4_CTE
        self.do_test(query)

    def test_Q2(self):
        query = Q2_subquery
        self.do_test(query)

    def test_Q71(self):
        query = Q71_subquery
        self.do_test(query)

    def test_Q54(self):
        query = Q54_subquery
        self.do_test(query)

    def test_Q5(self):
        query = Q5_CTE
        self.conn.config.detect_oj = True
        self.do_test(query)

    def test_Q11(self):
        query = Q11_CTE
        self.do_test(query)

    def test_Q74(self):
        query = Q74_subquery
        self.conn.config.detect_or = True
        self.do_test(query)


if __name__ == '__main__':
    unittest.main()
