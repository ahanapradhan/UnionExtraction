(Select s_suppkey as c_custkey, s_name as c_name
 From nation, supplier 
 Where n_nationkey = s_nationkey
 and n_name = 'CANADA')
 UNION ALL 
(Select c_custkey, c_name
 From customer, nation 
 Where c_nationkey = n_nationkey
 and n_name = 'UNITED STATES')
 UNION ALL 
(Select l_partkey as c_custkey, p_name as c_name
 From lineitem, part 
 Where l_partkey = p_partkey
 and l_quantity  >= 20.01);