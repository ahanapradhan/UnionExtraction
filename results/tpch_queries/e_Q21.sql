(Select s_name, Count(*) as numwait
From orders, supplier, lineitem, nation
Where s_suppkey = l_suppkey and s_nationkey = n_nationkey and o_orderkey = l_orderkey and o_orderstatus  = 'F' and n_name  = 'GERMANY'
Group By s_name
Order By s_name asc
Limit 100);