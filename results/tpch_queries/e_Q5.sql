(Select n_name, Sum(l_extendedprice-1.0*l_extendedprice*l_discount) as revenue
From region, orders, lineitem, nation, customer, supplier
Where s_suppkey = l_suppkey and s_nationkey = n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and r_regionkey = n_regionkey and r_name  = 'MIDDLE EAST' and o_orderdate  >= '1994-01-01' and o_orderdate <= '1994-12-31'
Group By n_name
Order By revenue asc, n_name asc
Limit 100);