Select s_name, Count(*) as numwait
From lineitem, nation, orders, supplier
Where l_suppkey = s_suppkey and n_nationkey = s_nationkey and l_orderkey = o_orderkey and o_orderstatus  = 'F' and n_name  = 'GERMANY'
Group By s_name
Order By s_name asc
Limit 100;