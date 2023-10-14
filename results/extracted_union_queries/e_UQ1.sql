(Select ps_suppkey as p_partkey, s_name as p_name
From partsupp, supplier
Where s_suppkey = ps_suppkey and ps_availqty  >= 202)
 UNION ALL 
(Select ps_partkey as p_partkey, p_name
From partsupp, part
Where p_partkey = ps_partkey and ps_availqty  >= 101);