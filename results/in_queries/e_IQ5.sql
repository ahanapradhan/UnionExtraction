(Select o_orderstatus, o_totalprice
From customer, orders
Where c_custkey = o_custkey and o_orderdate  <= '1995-03-09')
 INTERSECT 
(Select o_orderstatus, o_totalprice
From lineitem, orders
Where l_orderkey = o_orderkey and o_orderdate  >= '1995-03-11' and l_shipmode  = 'AIR');