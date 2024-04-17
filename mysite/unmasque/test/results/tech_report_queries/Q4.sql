Select o_orderdate, o_orderpriority, count(*) as order_count  
     From orders  
     Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month  
     Group By o_orderdate, o_orderpriority Order by o_orderpriority Limit 10;