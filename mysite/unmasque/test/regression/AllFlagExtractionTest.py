import random
import unittest
from datetime import date, timedelta

import pytest

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
        self.conn.config.detect_union = False
        self.conn.config.detect_nep = False
        self.conn.config.detect_oj = False
        self.conn.config.detect_or = False

        self.conn.config.use_cs2 = False
        self.pipeline = None

    def test_kaggle_Q15T1(self):
        query = """WITH time AS
  (
  SELECT DATE(tra_block_timestamp) AS trans_date
  FROM transactions
  )
  SELECT COUNT(1) AS transactions,
  trans_date
  FROM time
  GROUP BY trans_date
  ORDER BY trans_date;"""
        self.do_test(query)

    def test_kaggle_Q21T1(self):
        query = '''WITH c AS
  (
  SELECT com_parent_5, COUNT(*) as num_comments
  FROM comments_5
  GROUP BY com_parent_5
  )
  SELECT s.sto_id as story_id, s.sto_by, s.sto_title, c.num_comments
  FROM stories AS s
  LEFT JOIN c
  ON s.sto_id = c.com_parent_5
  WHERE s.sto_time_ts = '2012-01-01'
  ORDER BY c.num_comments DESC;'''
        self.conn.config.detect_oj = True
        self.do_test(query)

    def test_kaggle_Q21X3(self):
        query = '''SELECT u.use_id AS id, q.pq_title_4 as qtn_title, a.pos_body_5 as answer,
  MIN(q.pq_creation_date_4) AS q_creation_date,
  MIN(a.pos_creation_date_5) AS a_creation_date
  FROM posts_questions_4 AS q
  INNER JOIN posts_answers_5 AS a
  ON q.pq_owner_user_id_4 = a.pos_owner_user_id_5
  RIGHT JOIN users AS u
  ON q.pq_owner_user_id_4 = u.use_id
  WHERE u.use_creation_date >= '2019-01-01'
  and u.use_creation_date < '2019-02-01'
  GROUP BY u.use_id, q.pq_title_4, a.pos_body_5;'''
        self.conn.config.detect_oj = True
        self.do_test(query)

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
        self.assertTrue(self.pipeline.correct)
        del factory


if __name__ == '__main__':
    unittest.main()
