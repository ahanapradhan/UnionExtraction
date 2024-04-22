Select s_name, Count(*) as numwait
 From lineitem, nation, orders, supplier 
 Where lineitem.l_orderkey = orders.o_orderkey
 and nation.n_nationkey = supplier.s_nationkey
 and lineitem.l_suppkey = supplier.s_suppkey
 and orders.o_orderstatus = 'F'
 and nation.n_name = 'GERMANY' 
 Group By s_name 
 Order By numwait desc, s_name asc 
 Limit 100;