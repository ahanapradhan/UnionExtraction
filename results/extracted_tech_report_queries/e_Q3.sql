Select o_orderkey as l_orderkey, Sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, o_shippriority
 From customer, lineitem, orders 
 Where c_custkey = o_custkey
 and l_orderkey = o_orderkey
 and c_mktsegment = 'BUILDING'
 and o_orderdate <= '1995-03-14'
 and '1995-03-16' <= l_shipdate 
 Group By o_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;