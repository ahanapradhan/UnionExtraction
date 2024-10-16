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

    hq = """(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM
customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01'
GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL
(SELECT     l_orderkey as order_id,     AVG(l_quantity) AS total FROM     orders, lineitem where
l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     l_orderkey ORDER BY
total DESC LIMIT 10);"""
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = True
    conn.config.detect_oj = False
    conn.config.detect_nep = False
    conn.config.use_cs2 = False
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
