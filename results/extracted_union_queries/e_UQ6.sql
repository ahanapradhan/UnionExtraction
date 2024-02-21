(Select n_name as name, Sum(s_acctbal) as total_price
 From nation, supplier 
 Where n_nationkey = s_nationkey
 and n_name like '%UNITED%' 
 Group By n_name 
 Order By name desc)
 UNION ALL 
(Select o_clerk as name, Sum(l_extendedprice) as total_price
 From lineitem, orders 
 Where l_orderkey = o_orderkey
 and o_orderdate  <= '1995-01-01' 
 Group By o_clerk 
 Order By total_price desc, name desc 
 Limit 10);