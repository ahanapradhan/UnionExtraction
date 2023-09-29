(Select o_orderdate, o_orderpriority, Count(*) as order_count
From orders
Where o_orderdate  >= '1997-07-01' and o_orderdate <= '1997-10-01'
Group By o_orderdate, o_orderpriority
Order By o_orderpriority asc, o_orderdate asc
Limit 10);