Select o_orderkey as l_orderkey, Sum(l_extendedprice*(1 - l_discount)) as revenue, o_orderdate, o_shippriority
From customer, lineitem, orders
Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate  <= '1995-03-13' and l_shipdate  >= '1995-03-16' and c_mktsegment  = 'BUILDING'
Group By o_orderkey, o_orderdate, o_shippriority
Order By revenue asc, o_orderdate asc, l_orderkey asc, o_shippriority asc
Limit 10;