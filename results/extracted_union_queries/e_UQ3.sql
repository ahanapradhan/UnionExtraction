(Select c_custkey as key, c_name as name
 From customer, nation 
 Where c_nationkey = n_nationkey
 and n_name = 'UNITED STATES')
 UNION ALL 
(Select l_partkey as key, p_name as name
 From lineitem, part 
 Where l_partkey = p_partkey
 and l_quantity  >= 35.01);