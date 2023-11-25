import signal
import sys

from ..test.util import queries
from .pipeline.ExtractionPipeLine import ExtractionPipeLine
from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from .util.ConnectionHelper import ConnectionHelper


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    hq = "Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as " \
     "sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *" \
     "(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as " \
     "avg_price, avg(l_discount) as avg_disc, count(*) as count_order " \
     "From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' " \
     "Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;"

    hq = "SELECT avg(s_nationkey) FROM supplier WHERE s_suppkey >= 10 and s_suppkey <= 15;"

    conn = ConnectionHelper()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    pipeline = ExtractionPipeLine(conn)
    eq = pipeline.doJob(hq)

    print("=========== Extracted Query =============")
    print(eq)
    pipeline.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
