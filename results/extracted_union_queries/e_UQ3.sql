(Select p_partkey as key, p_name as name
From part, lineitem
Where p_partkey = l_partkey and l_quantity  >= 35.5)
 UNION ALL 
(Select c_custkey as key, c_name as name
From nation, customer
Where n_nationkey = c_nationkey and n_name  = 'UNITED STATES');