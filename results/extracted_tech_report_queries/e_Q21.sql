Select s_name, Count(*) as numwait
 From lineitem, nation, orders, supplier 
 Where n_nationkey = s_nationkey
 and l_suppkey = s_suppkey
 and l_orderkey = o_orderkey
 and n_name = 'GERMANY'
 and o_orderstatus = 'F' 
 Group By s_name 
 Order By numwait desc, s_name asc 
 Limit 100;