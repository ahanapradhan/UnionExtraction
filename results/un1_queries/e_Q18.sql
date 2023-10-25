Select c_name, o_orderdate, o_totalprice, Sum(l_quantity) as sum
From lineitem, orders, customer
Where c_custkey = o_custkey and o_orderkey = l_orderkey and c_phone LIKE '27-%'
Group By o_totalprice, o_orderdate, c_name
Order By o_orderdate asc, o_totalprice desc, c_name asc
Limit 100;