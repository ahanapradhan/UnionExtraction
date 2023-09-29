SELECT s_suppkey, s_name
FROM supplier, nation where s_nationkey = n_nationkey
and  n_name = 'GERMANY'
UNION ALL
SELECT c_custkey, c_name
FROM customer,
 orders where c_custkey = o_custkey
and o_orderpriority = '1-URGENT';