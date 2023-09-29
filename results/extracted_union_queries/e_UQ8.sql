(Select o_orderkey as order_id, Avg(l_quantity) as total
From orders, lineitem
Where o_orderkey = l_orderkey and o_orderdate  <= '1996-06-30'
Group By o_orderkey
Order By total desc
Limit 10)
 UNION ALL 
(Select c_custkey as order_id, Count(*) as total
From orders, customer
Where c_custkey = o_custkey and o_orderdate  >= '1995-01-01'
Group By c_custkey
Limit 10);