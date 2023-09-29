(Select o_clerk as name, Sum(l_extendedprice) as total_price
From lineitem, orders
Where o_orderkey = l_orderkey and o_orderdate  <= '1995-01-01'
Group By o_clerk
Order By total_price desc, name desc
Limit 10)
 UNION ALL 
(Select n_name as name, Sum(s_acctbal) as total_price
From supplier, nation
Where s_nationkey = n_nationkey and n_name LIKE '%UNITED%'
Group By n_name
Order By name desc);