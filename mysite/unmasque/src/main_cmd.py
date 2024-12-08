import signal
import sys

from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from ..src.core.factory.PipeLineFactory import PipeLineFactory
from ..src.util.ConnectionFactory import ConnectionHelperFactory


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


class TestQuery:
    def __init__(self, name: str, hidden_query: str, cs2: bool, union: bool, oj: bool, nep: bool, orf=None):
        self.qid = name
        self.cs2 = cs2
        self.union = union
        self.oj = oj
        self.nep = nep
        self.query = hidden_query
        self.orf = orf if orf is not None else False


def create_workload():
    test_workload = [TestQuery("Q_2_2_X_2", '''SELECT tax_pickup_community_area_5,  tax_trip_start_timestamp_5,  tax_trip_end_timestamp_5,  RANK()  
OVER (  PARTITION BY tax_pickup_community_area_5  ORDER BY tax_trip_start_timestamp_5  ) AS trip_number 
FROM taxi_trips_5 
WHERE DATE(tax_trip_start_timestamp_5) = '2017-05-01';''', False, False, False, False),

                     TestQuery("Q_2_1_T_1", '''WITH c AS
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
  ORDER BY c.num_comments DESC;''', False, False, True, False),

                     TestQuery("Q_2_1_T_2", """SELECT c.by
  FROM comments_5 AS c
  WHERE EXTRACT(DATE FROM c.time_ts) = '2014-01-01'
  UNION
  SELECT s.by
  FROM stories AS s
  WHERE EXTRACT(DATE FROM s.time_ts) = '2014-01-01'""", False, True, False, False),

                     TestQuery("Q_1_5_T_1", '''WITH time AS
  (
  SELECT DATE(tra_block_timestamp) AS trans_date
  FROM transactions
  )
  SELECT COUNT(1) AS transactions,
  trans_date
  FROM time
  GROUP BY trans_date
  ORDER BY trans_date;''', False, False, False, False),

                     TestQuery("Q_2_1_X_2", '''SELECT q.pq_owner_user_id_3 AS owner_user_id,
  MIN(q.pq_creation_date_3) AS q_creation_date,
  MIN(a.pos_creation_date_4) AS a_creation_date
  FROM posts_questions_3 AS q
  JOIN posts_answers_4 AS a
  ON q.pq_owner_user_id_3 = a.pos_owner_user_id_4
  WHERE q.pq_creation_date_3 >= '2019-01-01'
  AND q.pq_creation_date_3 < '2019-02-01'
  AND a.pos_creation_date_4 >= '2019-01-01'
  AND a.pos_creation_date_4 < '2019-02-01'
  GROUP BY q.pq_owner_user_id_3;''', False, False, False, False),

                     TestQuery("Q_2_1_X_3", '''SELECT u.use_id AS id, q.pq_title_4 as qtn_title, a.pos_body_5 as answer,
  MIN(q.pq_creation_date_4) AS q_creation_date,
  MIN(a.pos_creation_date_5) AS a_creation_date
  FROM posts_questions_4 AS q
  INNER JOIN posts_answers_5 AS a
  ON q.pq_owner_user_id_4 = a.pos_owner_user_id_5
  RIGHT JOIN users AS u
  ON q.pq_owner_user_id_4 = u.use_id
  WHERE u.use_creation_date >= '2019-01-01'
  and u.use_creation_date < '2019-02-01'
  GROUP BY u.use_id, q.pq_title_4, a.pos_body_5;''', False, False, True, False),

                     TestQuery("Q_2_1_X_4", '''SELECT q.pq_owner_user_id_5
  FROM posts_questions_5 AS q
  WHERE DATE(q.pq_creation_date_5) = '2019-01-01'
  UNION
  SELECT a.pos_owner_user_id_6
  FROM posts_answers_6 AS a
  WHERE DATE(a.pos_creation_date_6) = '2019-01-01';''', False, True, False, False)
                     ]
    return test_workload


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    workload = create_workload()

    workload_dict = {}
    for elem in workload:
        workload_dict[elem.qid] = workload.index(elem)

    # print(workload_dict)

    qid = sys.argv[1]
    hq = workload[workload_dict[qid]]
    query = hq.query
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = hq.union
    conn.config.detect_oj = hq.oj
    conn.config.detect_nep = hq.nep
    conn.config.use_cs2 = hq.cs2
    conn.config.detect_or = hq.orf

    print(f"Flags: Union {conn.config.detect_union}, OJ {conn.config.detect_oj}, "
          f"NEP {conn.config.detect_nep}, CS2 {conn.config.use_cs2}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    factory = PipeLineFactory()
    token = factory.init_job(conn, query)
    factory.doJob(query, token)
    result = factory.result

    if result is not None:
        print("============= Given Query ===============")
        print(query)
        print("=========== Extracted Query =============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()
    else:
        print("I had some Trouble! Check the log file for the details..")
