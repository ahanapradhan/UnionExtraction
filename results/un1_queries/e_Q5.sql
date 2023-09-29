Select n_name, Sum(l_extendedprice) as revenue
From customer, lineitem, orders, supplier, nation, region
Where s_suppkey = l_suppkey and s_nationkey = n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and r_regionkey = n_regionkey and o_orderdate  >= '1994-01-01' and o_orderdate <= '1994-12-31' and r_name  = 'MIDDLE EAST'
Group By n_name
Order By revenue desc, n_name asc
Limit 100;