(Select o_orderkey, l_shipdate as o_orderdate
From orders, lineitem
Where o_orderkey = l_orderkey and o_orderdate  <= '1993-12-31' and l_quantity  >= 20.5 and l_extendedprice  >= 1000.09)
 UNION ALL 
(Select o_orderkey, o_orderdate
From orders, customer
Where c_custkey = o_custkey and o_orderdate  >= '1997-01-01' and c_name LIKE '%0001248%');