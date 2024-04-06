Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue  
     From customer, orders, lineitem, supplier, nation, region  
     Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and  
     c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and  
     r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date  
     '1994-01-01' + interval '1' year  
     Group By n_name  
     Order by revenue desc Limit 100;