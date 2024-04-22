(Select c_custkey as order_id, Count(*) as total
 From customer, orders 
 Where c_custkey = o_custkey
 and o_orderdate  >= '1995-01-01' 
 Group By c_custkey 
 Order By total asc, order_id desc 
 Limit 10)
 UNION ALL 
(Select l_orderkey as order_id, Avg(l_quantity) as total
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and o_orderdate  <= '1996-06-30' 
 Group By l_orderkey 
 Order By total desc, order_id asc 
 Limit 10);