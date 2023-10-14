Select o_orderkey as l_orderkey, Sum(l_extendedprice-1.0*l_extendedprice*l_discount) as revenue, o_orderdate, o_shippriority
From orders, lineitem, customer
Where c_custkey = o_custkey and o_orderkey = l_orderkey and o_orderdate  <= '1995-03-14' and l_shipdate  >= '1995-03-17' and c_mktsegment  = 'BUILDING'
Group By o_orderkey, o_orderdate, o_shippriority
Order By revenue asc, o_orderdate asc
Limit 10;