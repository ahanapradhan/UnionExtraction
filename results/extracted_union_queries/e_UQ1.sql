(Select p_partkey, p_name
 From part, partsupp 
 Where p_partkey = ps_partkey
 and ps_availqty  >= 101)
 UNION ALL 
(Select s_suppkey as p_partkey, s_name as p_name
 From partsupp, supplier 
 Where ps_suppkey = s_suppkey
 and ps_availqty  >= 201);