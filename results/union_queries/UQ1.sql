(SELECT p_partkey, p_name
FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100)
UNION ALL
(SELECT s_suppkey, s_name
FROM supplier, partsupp where s_suppkey = ps_suppkey
and ps_availqty > 200);