IQ1 = "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
      "AND n_name = 'BRAZIL' INTERSECT " \
      "SELECT c_mktsegment as segment FROM customer, nation WHERE n_nationkey = c_nationkey " \
      "AND n_name = 'ARGENTINA';"

IQ2 = "select c_mktsegment as segment from customer,nation " \
      "where c_acctbal < 3000 and c_nationkey = n_nationkey and n_name = 'BRAZIL' " \
      "intersect " \
      "select c_mktsegment as segment from customer,nation,orders where " \
      "c_acctbal between 1000 and 5000 and c_nationkey = n_nationkey and c_custkey = o_custkey " \
      "and n_name = 'ARGENTINA';"

IQ3 = "select l_shipdate as checked_date from lineitem, orders " \
      "where l_orderkey = o_orderkey  " \
      "and o_orderstatus = 'F' " \
      "INTERSECT " \
      "select o_orderdate as checked_date from orders, lineitem, customer " \
      "where l_orderkey = o_orderkey " \
      "and o_custkey = c_custkey " \
      "and o_orderstatus = 'O' " \
      "and l_shipmode = 'RAIL' and c_acctbal < 1000;"

IQ4 = "select l_shipdate as checked_date, l_returnflag, l_shipinstruct from lineitem, orders " \
      "where l_orderkey = o_orderkey  " \
      "and o_orderstatus = 'F' " \
      "INTERSECT " \
      "select o_orderdate as checked_date, l_returnflag, l_shipinstruct from orders, lineitem, customer " \
      "where l_orderkey = o_orderkey " \
      "and o_custkey = c_custkey " \
      "and o_orderstatus = 'O' " \
      "and l_shipmode = 'RAIL' and c_acctbal < 1000;"

IQ5 = "select o_orderstatus, o_totalprice " \
      "from customer,orders where c_custkey = o_custkey and o_orderdate < date '1995-03-10' " \
      "intersect " \
      "select o_orderstatus, o_totalprice from lineitem, orders " \
      "where o_orderkey = l_orderkey and o_orderdate > date '1995-03-10' and l_shipmode = 'AIR';"

IQ6 = "select p_container,p_retailprice,ps_availqty " \
      "from part,supplier,partsupp where p_partkey = ps_partkey and s_suppkey = ps_suppkey and " \
      "p_brand='Brand#45' intersect select p_container,p_retailprice,ps_availqty " \
      "from part,supplier,partsupp where p_partkey = ps_partkey and s_suppkey=ps_suppkey and " \
      "p_brand='Brand#15';"
