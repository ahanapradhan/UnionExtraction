import signal
import sys

from .util.ConnectionFactory import ConnectionHelperFactory
from .core.factory.PipeLineFactory import PipeLineFactory
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
    hq = """
    select sum(cs_ext_discount_amt) as "excess_discount_amount"
	from catalog_sales, item, date_dim 
	where i_manufact_id = 722 
	and i_item_sk = cs_item_sk 
	and d_date between DATE '2002-03-09' and DATE '2002-03-09' + interval '90 days'
    and d_date_sk = cs_sold_date_sk 
	and cs_ext_discount_amt > 10 
	and d_date_sk = cs_sold_date_sk;
    """

    #hq = """select s_store_id from store where s_number_employees > 5 limit 10;
    #"""
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = False
    conn.config.detect_oj = False
    conn.config.detect_nep = False
    conn.config.detect_or = False
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
