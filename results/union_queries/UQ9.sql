SELECT c_name as name, c_acctbal as account_balance
FROM orders, customer, nation
WHERE c_custkey = o_custkey
and c_nationkey = n_nationkey
and c_mktsegment = 'FURNITURE'
and n_name = 'INDIA'
and o_orderdate between '1998-01-01' and '1998-01-05'
and o_totalprice <= c_acctbal

UNION ALL

SELECT s_name as name,
s_acctbal as account_balance
FROM supplier, lineitem, orders, nation
WHERE l_suppkey = s_suppkey
and l_orderkey = o_orderkey
and s_nationkey = n_nationkey and n_name = 'ARGENTINA'
and o_orderdate between '1998-01-01' and '1998-01-05'
and o_totalprice >= s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal;
