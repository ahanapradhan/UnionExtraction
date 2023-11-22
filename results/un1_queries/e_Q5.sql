Select n_name, Sum(l_extendedprice*(1 - l_discount)) as revenue
From customer, lineitem, nation, orders, region, supplier
Where l_suppkey = s_suppkey and c_nationkey = n_nationkey and n_nationkey = s_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey and n_regionkey = r_regionkey and o_orderdate  >= '1994-01-02' and o_orderdate <= '1995-01-01' and r_name  = 'MIDDLE EAST'
Group By n_name
Order By revenue asc, n_name asc
Limit 100;