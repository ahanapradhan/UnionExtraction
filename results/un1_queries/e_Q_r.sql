Select c_mktsegment as segment
From customer, nation, orders
Where c_nationkey = n_nationkey and c_custkey = o_custkey and c_acctbal  >= 999.995 and c_acctbal <= 4999.999 and n_name NOT LIKE 'B%' ;