import signal
import sys

from .util.workload_queries import create_workload, TestQuery
from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from .core.factory.PipeLineFactory import PipeLineFactory
from .util.ConnectionFactory import ConnectionHelperFactory

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
    workload = create_workload()

    workload_dict = {}
    for elem in workload:
        workload_dict[elem.qid] = workload.index(elem)

    # print(workload_dict)

    # qid = sys.argv[1]
    # hq = workload[workload_dict[qid]]
    # hq = TestQuery("testing", """SELECT SUM(revenue) AS revenue FROM (SELECT l_extendedprice * l_discount
    # AS revenue FROM lineitem WHERE l_shipdate >= DATE '1993-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount
    # BETWEEN 0.05 AND 0.07 AND l_quantity < 24) AS combined_revenue;""", False, False,False, False)
    #hq = TestQuery("testing", """(select n_name from nation) union all (select r_name from region);""", False, True, False, False)
    # hq = TestQuery("testing", """select p_type from part where p_brand in ('Brand#13','Brand#15');""", False, False,
    #                False, False, True)
    # hq = TestQuery("testing", """select n_name from nation where n_name != 'BRAZIL';""", False, False,
    #                False, True)
    # hq = TestQuery("testing", """select l_extendedprice * l_discount as revenue from lineitem where l_shipdate >= date '1993-01-01'
    # and l_shipdate < date '1994-03-01' + interval '1' year and l_discount <0 and l_quantity < 10;""",False, False, False, False)
    # hq = TestQuery("testing", """select B, count(*) as custdist from ( select c_custkey, o_orderdate from customer join orders on c_custkey = o_custkey
    # and o_comment not like '%special%requests%' group by c_custkey, o_orderdate ) as c_orders (A, B) group by B order by custdist desc, B desc;""", False, False, False, False)
    hq = TestQuery("testing", """SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
    FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND
    l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10;""", False, False, False, False)
    query = hq.query
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = hq.union
    conn.config.detect_oj = hq.oj
    conn.config.detect_nep = hq.nep
    conn.config.use_cs2 = hq.cs2
    conn.config.detect_or = hq.orf
    error_output = False

    print(f"Flags: Union {conn.config.detect_union}, OJ {conn.config.detect_oj}, "
          f"NEP {conn.config.detect_nep}, CS2 {conn.config.use_cs2}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    factory = PipeLineFactory()
    token = factory.init_job(conn, query)
    factory.doJob(query, token)
    result = factory.result

    if result is not None:
        print("============== Given Query ==============")
        print(query)
        print("============ Extracted Query ============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()
    else:
        print("I had some Trouble! Check the log file for the details..")
