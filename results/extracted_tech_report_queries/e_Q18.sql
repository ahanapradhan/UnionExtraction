Select c_name, o_orderdate, o_totalprice, Sum(l_quantity) as sum
 From customer, lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_phone LIKE '27-%' 
 Group By o_totalprice, o_orderdate, c_name 
 Order By o_orderdate asc, o_totalprice desc, c_name asc 
 Limit 100;