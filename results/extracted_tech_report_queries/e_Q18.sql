Select c_name, o_orderdate, o_totalprice, Sum(l_quantity) as sum
 From customer, lineitem, orders 
 Where l_orderkey = o_orderkey
 and c_custkey = o_custkey
 and c_phone LIKE '27-%' 
 Group By o_orderdate, o_totalprice, c_name 
 Order By o_orderdate asc, o_totalprice desc, c_name asc 
 Limit 100;