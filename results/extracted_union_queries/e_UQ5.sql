(Select l_orderkey as o_orderkey, l_shipdate as o_orderdate
From lineitem, orders
Where o_orderkey = l_orderkey and l_quantity  >= 21.0 and l_extendedprice  >= 1001.0 and o_orderdate  <= '1993-12-31')
 UNION ALL 
(Select o_orderkey, o_orderdate
From customer, orders
Where c_custkey = o_custkey and c_name LIKE '%0001248%' and o_orderdate  >= '1997-01-01');