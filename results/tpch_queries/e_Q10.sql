(Select c_name, Sum(l_extendedprice-1.0*l_extendedprice*l_discount) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment
From orders, customer, nation, lineitem
Where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and o_orderdate  >= '1994-01-01' and o_orderdate <= '1994-03-31' and l_returnflag  = 'R'
Group By c_name, c_address, c_phone, c_acctbal, c_comment, n_name
Order By revenue asc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc
Limit 20);