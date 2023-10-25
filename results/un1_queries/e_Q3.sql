Select l_orderkey, Sum(l_extendedprice-1.0*l_extendedprice*l_discount) as revenue, o_orderdate, o_shippriority
From lineitem, orders, customer
Where c_custkey = o_custkey and o_orderkey = l_orderkey and l_shipdate  >= '1995-03-16' and o_orderdate  <= '1995-03-14' and c_mktsegment  = 'BUILDING'
Group By l_orderkey, o_orderdate, o_shippriority
Order By revenue asc, o_orderdate asc
Limit 10;