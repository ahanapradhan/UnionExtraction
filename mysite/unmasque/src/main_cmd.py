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
    hq = "Select l_returnflag, l_linestatus, Count(*) as count_order From lineitem Where l_shipdate >= '1998-07-07' " \
         "Group By l_returnflag, l_linestatus " \
         "Order By l_returnflag asc, l_linestatus asc Limit 10;"
    hq = queries.Q3_1
    conn = ConnectionHelper()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    pipeline = ExtractionPipeLine(conn)
    eq = pipeline.doJob(hq)

    print("=========== Extracted Query =============")
    print(eq)
    pipeline.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
