Select c_name, o_orderdate, o_totalprice, Sum(l_quantity) as sum
From customer, orders, lineitem
Where c_custkey = o_custkey and o_orderkey = l_orderkey and c_phone LIKE '27-%'
Group By c_name, o_totalprice, o_orderdate
Order By o_orderdate asc, o_totalprice desc, c_name asc
Limit 100;