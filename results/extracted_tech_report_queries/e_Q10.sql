Select c_name, Sum(l_extendedprice*(1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment
 From customer, lineitem, nation, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation.n_nationkey
 and lineitem.l_returnflag = 'R'
 and orders.o_orderdate  >= '1994-01-01' and orders.o_orderdate <= '1994-03-31' 
 Group By n_name, c_name, c_address, c_phone, c_acctbal, c_comment 
 Order By revenue desc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc 
 Limit 20;