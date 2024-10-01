 
 Select l_orderkey as order_id, Avg(l_quantity) as total 
 From customer, lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1996-06-30' 
 Group By l_orderkey 
 Order By total desc, order_id asc 
 Limit 10;