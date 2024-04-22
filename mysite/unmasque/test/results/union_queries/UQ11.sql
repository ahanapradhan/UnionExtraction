Select o_orderpriority,
count(*) as order_count
From orders, lineitem
Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01'
and o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate
Group By o_orderpriority
Order By o_orderpriority;