import copy
import signal
import sys

from ..src.core.executables.executable import Executable
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


class TestQuery:
    def __init__(self, name: str, query: str, cs2: bool, union: bool, oj: bool, nep: bool):
        self.qid = name
        self.cs2 = cs2
        self.union = union
        self.oj = oj
        self.nep = nep
        self.query = query


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    workload = [TestQuery("U1", """(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100
Order By p_partkey Limit 5)
UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and
ps_availqty > 200 Order By s_suppkey Limit 7);""", False, True, False, False),
                TestQuery("U2", """(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' order by s_suppkey desc, s_name limit 12)UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT' order by c_custkey, c_name desc limit 10);
""", False, True, False, False),
                TestQuery("U3", """(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' Order by key Limit 10)
 UNION ALL (SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity > 35 Order By key Limit 10) 
 UNION ALL (select n_nationkey as key, r_name as name from nation, region where n_name LIKE 'B%' Order By key Limit 5)
""", False, True, False, False),
                TestQuery("U4", """(SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' Order By c_custkey desc Limit 5) 
 UNION ALL (SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' Order By s_suppkey Limit 6) 
 UNION ALL (SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20 Order By p_partkey desc Limit 7) 
 UNION ALL (SELECT ps_partkey, p_name FROM part ,  partsupp where p_partkey = ps_partkey and ps_supplycost >= 1000 Order By ps_partkey Limit 8)
""", False, True, False, False),
                TestQuery("U5", """(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' order by o_orderkey Limit 20) 
 UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);
""", False, True, False, False),
                TestQuery("U6", """(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) 
 UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC Limit 10);
""", False, True, False, False),
                TestQuery("U7", """(SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  Order By key Limit 20)
 UNION ALL  (SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost < 100 Order By price Limit 20);
""", False, True, False, False),
                TestQuery("U8", """(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM
customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01'
GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL
(SELECT     l_orderkey as order_id,     AVG(l_quantity) AS total FROM     orders, lineitem where
l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     l_orderkey ORDER BY
total DESC LIMIT 10);
""", False, True, False, False),
                TestQuery("U9", """(select c_name, n_name from customer, nation where
c_mktsegment='BUILDING' and c_acctbal > 100 and c_nationkey
= n_nationkey) UNION ALL (select s_name, n_name from supplier,
nation where s_acctbal > 4000 and s_nationkey = n_nationkey);""", False, True, False, False),
                TestQuery("O1", """""", True, False, True, False),
                TestQuery("O2", """""", True, False, True, False),
                TestQuery("O3", """""", True, False, True, False),
                TestQuery("O4", """""", True, False, True, False),
                TestQuery("O5", """""", True, False, True, False),
                TestQuery("O6", """""", True, False, True, False),
                TestQuery("A1", """""", True, False, False, False),
                TestQuery("A2", """""", True, False, False, False),
                TestQuery("A3", """""", True, False, False, False),
                TestQuery("A4", """""", True, False, False, False),
                TestQuery("A5", """""", True, False, False, False),
                TestQuery("N1", """""", True, False, False, False),
                TestQuery("F1", """""", True, True, True, False),
                TestQuery("F2", """""", True, True, True, False),
                TestQuery("F3", """""", True, True, True, False),
                TestQuery("F4", """""", True, True, True, True)]

    workload_dict = {}
    for elem in workload:
        workload_dict[elem.qid] = workload.index(elem)

    hq = workload[workload_dict["U2"]]
    conn = ConnectionHelperFactory().createConnectionHelper()
    query = hq.query
    conn.config.detect_union = hq.union
    conn.config.detect_oj = hq.oj
    conn.config.detect_nep = hq.nep
    conn.config.use_cs2 = hq.nep
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    factory = PipeLineFactory()

    conn.connectUsingParams()
    app = Executable(conn)
    res = app.doJob(query)
    conn.closeConnection()
    exe_time = copy.deepcopy(app.local_elapsed_time)

    token = factory.init_job(conn, query)
    factory.doJob(query, token)
    result = factory.result

    if result is not None:
        print("Union P = " + str(conn.config.detect_union) + "   " + "Outer Join P = " + str(conn.config.detect_oj))
        print("NEP P = " + str(conn.config.detect_nep) + "   " + "Or P = " + str(conn.config.detect_or))
        print(f"========= HQ Execution Time: {round(exe_time, 2)}(s) =============")
        print("============= Given Query ===============")
        print(query)
        print("=========== Extracted Query =============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
