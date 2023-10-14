(Select c_custkey as key, c_name as name
From customer, nation
Where n_nationkey = c_nationkey and n_name  = 'UNITED STATES')
 UNION ALL 
(Select l_partkey as key, p_name as name
From lineitem, part
Where p_partkey = l_partkey and l_quantity  >= 36.0);