(Select s_name as name, s_acctbal as account_balance
 From lineitem, nation, orders, supplier 
 Where l_orderkey = o_orderkey
 and l_suppkey = s_suppkey
 and n_nationkey = s_nationkey
 and n_name = 'ARGENTINA'
 and o_orderdate  >= '1998-01-01' and o_orderdate <= '1998-01-05'
 and s_acctbal <= o_totalprice
 and 29999.995 <= o_totalprice
 and s_acctbal <= 50000.004)
 UNION ALL 
(Select c_name as name, c_acctbal as account_balance
 From customer, nation, orders 
 Where c_nationkey = n_nationkey
 and c_custkey = o_custkey
 and n_name = 'INDIA'
 and c_mktsegment = 'FURNITURE'
 and o_orderdate  >= '1998-01-01' and o_orderdate <= '1998-01-05'
 and o_totalprice <= c_acctbal);