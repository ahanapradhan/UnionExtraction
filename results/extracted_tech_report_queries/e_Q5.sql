Select n_name, Sum(l_extendedprice*(1 - l_discount)) as revenue
 From customer, lineitem, nation, orders, region, supplier 
 Where c_nationkey = n_nationkey
 and n_nationkey = s_nationkey
 and n_regionkey = r_regionkey
 and l_suppkey = s_suppkey
 and l_orderkey = o_orderkey
 and c_custkey = o_custkey
 and r_name = 'MIDDLE EAST'
 and o_orderdate  >= '1994-01-01' and o_orderdate <= '1994-12-31' 
 Group By n_name 
 Order By revenue desc, n_name asc 
 Limit 100;