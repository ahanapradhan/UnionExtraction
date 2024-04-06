Select s_name, Count(*) as numwait
 From lineitem, nation, orders, supplier 
 Where l_orderkey = o_orderkey
 and l_suppkey = s_suppkey
 and n_nationkey = s_nationkey
 and o_orderstatus = 'F'
 and n_name = 'GERMANY' 
 Group By s_name 
 Order By numwait desc, s_name asc 
 Limit 100;