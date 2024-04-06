Select l_orderkey, Sum(-l_discount*l_extendedprice + l_extendedprice) as revenue, o_orderdate, o_shippriority
 From customer, lineitem, orders 
 Where l_orderkey = o_orderkey
 and c_custkey = o_custkey
 and c_mktsegment = 'BUILDING'
 and '1995-03-16' <= l_shipdate
 and o_orderdate <= '1995-03-14' 
 Group By l_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;