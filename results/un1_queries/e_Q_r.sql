Select c_mktsegment as segment
From customer, nation, orders
Where c_nationkey = n_nationkey and c_custkey = o_custkey and c_acctbal  >= 1000.0 and c_acctbal <= 5000.002 and n_name NOT LIKE 'B%' ;