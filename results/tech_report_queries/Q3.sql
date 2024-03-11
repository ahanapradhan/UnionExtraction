Select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority  
     From customer, orders, lineitem  
     Where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and  
     o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'  
     Group By l_orderkey, o_orderdate, o_shippriority  
     Order by revenue desc, o_orderdate Limit 10;