(Select s_name, Count(*) as numwait
From nation, lineitem, orders, supplier
Where s_suppkey = l_suppkey and s_nationkey = n_nationkey and o_orderkey = l_orderkey and n_name  = 'GERMANY' and o_orderstatus  = 'F'
Group By s_name
Order By s_name asc
Limit 100);