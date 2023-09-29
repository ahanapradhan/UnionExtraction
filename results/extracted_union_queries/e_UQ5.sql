(Select o_orderkey, l_shipdate as o_orderdate
From lineitem, orders
Where o_orderkey = l_orderkey and l_quantity  >= 20.5 and l_extendedprice  >= 1000.04 and o_orderdate  <= '1994-01-01')
 UNION ALL 
(Select o_orderkey, o_orderdate
From orders, customer
Where c_custkey = o_custkey and o_orderdate  >= '1997-01-01' and c_name LIKE '%0001248%');