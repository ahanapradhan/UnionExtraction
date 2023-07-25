tpch_query1 = "select count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval ':1' " \
              "day group by l_returnflag, l_linestatus;"

tpch_query3 = "select c_mktsegment, l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, " \
              "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
              "and l_orderkey = o_orderkey and o_orderdate > date '1995-10-11' " \
              "group by l_orderkey, o_orderdate, o_shippriority, c_mktsegment limit 4;"
