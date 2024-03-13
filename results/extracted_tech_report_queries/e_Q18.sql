Select c_name, o_orderdate, o_totalprice, Sum(l_quantity) as sum
 From customer, lineitem, orders 
 Where c_custkey = o_custkey
 and l_orderkey = o_orderkey
 and c_phone LIKE '27-%' 
 Group By c_name, o_totalprice, o_orderdate 
 Order By c_name asc, o_orderdate asc, o_totalprice desc 
 Limit 100;