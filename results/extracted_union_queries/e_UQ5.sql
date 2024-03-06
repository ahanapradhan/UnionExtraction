(Select o_orderkey, l_shipdate as o_orderdate
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and o_orderdate  <= '1993-12-31'
 and 20.01 <= l_quantity
 and 1000.01 <= l_extendedprice)
 UNION ALL 
(Select o_orderkey, o_orderdate
 From customer, orders 
 Where c_custkey = o_custkey
 and c_name LIKE '%0001248%'
 and o_orderdate  >= '1997-01-01');