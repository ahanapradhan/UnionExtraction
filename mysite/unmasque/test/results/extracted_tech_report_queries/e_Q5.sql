Select n_name, Sum(l_extendedprice*(1 - l_discount)) as revenue
 From customer, lineitem, nation, orders, region, supplier 
 Where lineitem.l_orderkey = orders.o_orderkey
 and customer.c_custkey = orders.o_custkey
 and lineitem.l_suppkey = supplier.s_suppkey
 and customer.c_nationkey = nation.n_nationkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and region.r_name = 'MIDDLE EAST'
 and orders.o_orderdate  >= '1994-01-01' and orders.o_orderdate <= '1994-12-31' 
 Group By n_name 
 Order By revenue desc, n_name asc 
 Limit 100;