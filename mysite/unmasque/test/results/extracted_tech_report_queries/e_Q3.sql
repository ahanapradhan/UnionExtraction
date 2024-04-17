Select o_orderkey as l_orderkey, Sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, o_shippriority
 From customer, lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_mktsegment = 'BUILDING'
 and orders.o_orderdate <= '1995-03-14'
 and '1995-03-16' <= lineitem.l_shipdate 
 Group By o_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;