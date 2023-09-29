(Select s_suppkey, s_name
From supplier, nation
Where s_nationkey = n_nationkey and n_name  = 'GERMANY')
 UNION ALL 
(Select c_custkey as s_suppkey, c_name as s_name
From customer, orders
Where c_custkey = o_custkey and o_orderpriority  = '1-URGENT');