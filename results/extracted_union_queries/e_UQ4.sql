(Select c_custkey, c_name
From customer, nation
Where n_nationkey = c_nationkey and n_name  = 'UNITED STATES')
 UNION ALL 
(Select l_partkey as c_custkey, p_name as c_name
From lineitem, part
Where p_partkey = l_partkey and l_quantity  >= 21.0)
 UNION ALL 
(Select s_suppkey as c_custkey, s_name as c_name
From supplier, nation
Where s_nationkey = n_nationkey and n_name  = 'CANADA');