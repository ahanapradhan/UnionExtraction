SELECT c_custkey, c_name
FROM customer,
 nation where c_nationkey = n_nationkey
and n_name = 'UNITED STATES'
UNION ALL
SELECT s_suppkey, s_name
FROM supplier ,
 nation where s_nationkey = n_nationkey
and n_name = 'CANADA'
UNION ALL
SELECT p_partkey, p_name
FROM part ,
 lineitem where p_partkey = l_partkey
and l_quantity > 20;