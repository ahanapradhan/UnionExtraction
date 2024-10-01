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
    """   
        ()
    UNION ALL (SELECT s_name as entity_name, n_name as country,
    avg(l_extendedprice*(1 - l_discount)) as price
    FROM supplier, lineitem, orders, nation, region
    WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and o_totalprice > s_acctbal and o_totalprice <= 15000
    and r_name <> 'EUROPE'
    group by n_name, s_name
    order by price desc, country desc, entity_name asc limit 10);
    """

    hq = """ 
    SELECT c_name as entity_name, n_name as country, o_totalprice as price
from orders LEFT OUTER JOIN customer on c_custkey = o_custkey
and c_acctbal >= o_totalprice and c_acctbal >= 9000
LEFT OUTER JOIN nation ON c_nationkey = n_nationkey  
	where o_totalprice <= 15000
group by n_name, c_name, o_totalprice
order by price, country asc, entity_name desc limit 20;
"""
    hq = """
    SELECT s_name as entity_name, n_name as country,
    avg(l_extendedprice*(1 - l_discount)) as price
    FROM supplier, lineitem, orders, nation, region
    WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and o_totalprice > s_acctbal and o_totalprice <= 15000
    and r_name <> 'EUROPE'
    group by n_name, s_name
    order by price desc, country desc, entity_name asc limit 10;"""

    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = False
    conn.config.detect_oj = True
    conn.config.detect_nep = True
    conn.config.detect_or = False
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
