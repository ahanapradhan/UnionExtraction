import signal
import sys
from .pipeline.ExtractionPipeLine import ExtractionPipeLine
from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from .util.ConnectionHelper import ConnectionHelper


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
    hq = "select l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, " \
         "count(*) as count_order from lineitem group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 10;"

    conn = ConnectionHelper()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    pipeline = ExtractionPipeLine(conn)
    eq = pipeline.doJob(hq)

    print("=========== Extracted Query =============")
    print(eq)
    pipeline.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
