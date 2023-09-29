(Select s_suppkey as p_partkey, s_name as p_name
From supplier, partsupp
Where s_suppkey = ps_suppkey and ps_availqty  >= 201)
 UNION ALL 
(Select p_partkey, p_name
From part, partsupp
Where p_partkey = ps_partkey and ps_availqty  >= 101);