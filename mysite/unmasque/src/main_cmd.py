import signal
import sys

from ..src.util.ConnectionFactory import ConnectionHelperFactory
from mysite.unmasque.src.core.factory.PipeLineFactory import PipeLineFactory
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
    hq = "Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue " \
         "From customer, orders, lineitem, supplier, nation, region " \
         "Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and " \
         "c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and " \
         "r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date " \
         "'1994-01-01' + interval '1' year " \
         "Group By n_name " \
         "Order by revenue desc Limit 100;"

    hq = "select l_orderkey, sum(l_extendedprice*(1 - l_discount) - o_totalprice) as revenue, o_orderdate, " \
         "o_shippriority  from customer, orders, " \
         "lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and " \
         "o_orderdate " \
         "< '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by " \
         "revenue " \
         "desc, o_orderdate limit 10;"

    hq = "select AVG(l_extendedprice) as avgTOTAL from lineitem,part " \
         "where p_partkey = l_partkey and (p_brand = 'Brand#52') and " \
         "(p_container = 'LG CAN');"

    # hq = "select c_name from customer UNION ALL select s_name from supplier UNION ALL select n_name from nation;"

    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = False
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    factory = PipeLineFactory()
    token = factory.init_job(conn, hq)
    factory.doJob(hq, token)
    result = factory.result

    if result is not None:
        print("=========== Extracted Query =============")
        print(result)
        print("============= Profile ===================")
        pipe = factory.get_pipeline_obj(token)
        print(pipe.time_profile.print())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
