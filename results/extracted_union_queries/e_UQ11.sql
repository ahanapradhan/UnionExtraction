Select o_orderpriority, Count(*) as order_count
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and l_commitdate <= l_receiptdate
 and '1993-07-01' <= o_orderdate
 and o_orderdate <= '1993-09-30' 
 Group By o_orderpriority 
 Order By o_orderpriority asc;