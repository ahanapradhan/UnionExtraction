import signal
import sys

from ..src.util.ConnectionFactory import ConnectionHelperFactory
from ..src.core.factory.PipeLineFactory import PipeLineFactory
from .pipeline.abstract.TpchSanitizer import TpchSanitizer


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    hq = """SELECT MIN(n_name) AS of_person,
       MIN(t_title) AS biography_movie
FROM aka_name,
     cast_info,
     info_type,
     link_type,
     movie_link,
     name,
     person_info,
     title
WHERE an_name LIKE '%a%'
  AND it_info ='mini biography'
  AND lt_link ='features'
  AND pi_note ='Volker Boehm'
  AND t_production_year BETWEEN 1980 AND 1995
  AND n_id = an_person_id
  AND n_id = pi_person_id
  AND ci_person_id = n_id
  AND t_id = ci_movie_id
  AND ml_linked_movie_id = t_id
  AND lt_id = ml_link_type_id
  AND it_id = pi_info_type_id
AND pi_person_id = an_person_id
  AND pi_person_id = ci_person_id
  AND an_person_id = ci_person_id
  AND ci_movie_id = ml_linked_movie_id;
    """
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = False
    conn.config.detect_oj = False
    conn.config.detect_nep = False
    conn.config.use_cs2 = False
    # conn.config.detect_or = True

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    factory = PipeLineFactory()
    token = factory.init_job(conn, hq)
    factory.doJob(hq, token)
    result = factory.result

    if result is not None:
        print("Union P = " + str(conn.config.detect_union) + "   " + "Outer Join P = " + str(conn.config.detect_oj))
        print("NEP P = " + str(conn.config.detect_nep) + "   " + "Or P = " + str(conn.config.detect_or))
        print("============= Given Query ===============")
        print(hq)
        print("=========== Extracted Query =============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
