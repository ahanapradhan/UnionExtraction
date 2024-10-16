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

    hq = """(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' order by o_orderkey Limit 20) 
 UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);"""

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
