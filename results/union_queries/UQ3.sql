SELECT c_custkey as key, c_name as name
FROM customer, nation where c_nationkey = n_nationkey and
 n_name = 'UNITED STATES'
UNION ALL
SELECT p_partkey as key, p_name as name
FROM part , lineitem where p_partkey = l_partkey
and l_quantity > 35;