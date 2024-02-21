Select o_orderpriority, Count(*) as order_count
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and l_commitdate <= l_receiptdate
 and l_commitdate <= '1994-01-06'
 and '1993-07-01' <= o_orderdate
 and o_orderdate <= '1993-09-30' 
 Group By o_orderpriority 
 Order By o_orderpriority asc;