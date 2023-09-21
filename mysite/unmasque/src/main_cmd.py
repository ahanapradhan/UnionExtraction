import signal
import sys

from mysite.unmasque.src.pipeline.ExtractionPipeLine import ExtractionPipeLine
from mysite.unmasque.src.pipeline.abstract.TpchSanitizer import TpchSanitizer
from mysite.unmasque.src.util.ConnectionHelper import ConnectionHelper


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.doJob()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    hq = "select c_mktsegment, l_orderkey, sum(l_extendedprice) as revenue, " \
         "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
         "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
         "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"

    conn = ConnectionHelper()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    pipeline = ExtractionPipeLine(conn)
    eq = pipeline.extract(hq)

    print("=========== Extracted Query =============")
    print(eq)
    pipeline.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
