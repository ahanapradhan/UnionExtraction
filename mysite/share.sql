SELECT n_name as name, c_acctbal as account_balance FROM tpch.orders NATURAL JOIN tpch.customer NATURAL JOIN tpch.nation WHERE o_totalprice <= 70000

/* Extracted Query: */
Select  as NAME,  as ACCOUNT_BALANCE
 From customer, nation, orders 
 Where customer.custkey = orders.custkey
 and customer.nationkey = nation.nationkey
 and customer.c_name = 'Customer#000002900'
 and customer.c_address = 'xeicQEyv6I'
 and customer.c_phone = '19-292-999-1038'
 and customer.c_acctbal = 4794.87
 and customer.c_mktsegment = 'HOUSEHOLD'
 and customer.c_comment = 'ironic packages. pending, regular deposits cajole blithely. carefully even instructions engage stealthily carefull'
 and nation.n_name = 'INDONESIA'
 and nation.regionkey = 2
 and nation.n_comment = 'slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'
 and orders.orderkey = 55620
 and orders.o_orderstatus = 'O'
 and orders.o_totalprice = 2590.94
 and orders.o_orderpriority = '4-NOT SPECIFIED'
 and orders.o_clerk = 'Clerk#000000311'
 and orders.o_shippriority = 0
 and orders.o_comment = 'usly. regular, regul';


Select o_orderstatus, l_shipmode From tpch.lineitem natural join tpch.orders Where l_linenumber >= 4 and o_clerk LIKE 'Clerk%61' 

/* Extracted Query: */
Select o_orderstatus as O_ORDERSTATUS,  as L_SHIPMODE
 From lineitem, orders 
 Where lineitem.orderkey = orders.orderkey
 and lineitem.l_linestatus = orders.o_orderstatus
 and orders.custkey = 23014
 and orders.o_totalprice = 168982.73
 and orders.o_orderpriority = '5-LOW'
 and orders.o_clerk = 'Clerk#000000061'
 and orders.o_shippriority = 0
 and orders.o_comment = 'ost slyly around the blithely bold requests.'
 and lineitem.partkey = 2997
 and lineitem.suppkey = 4248
 and lineitem.l_linenumber = 6
 and lineitem.l_quantity = 19.0
 and lineitem.l_extendedprice = 36099.81
 and lineitem.l_discount = 0.04
 and lineitem.l_tax = 0.08
 and lineitem.l_returnflag = 'R'
 and lineitem.l_shipinstruct = 'NONE'
 and lineitem.l_shipmode = 'AIR'
 and lineitem.l_comment = 're. unusual frets after the sl' 
 Order By L_SHIPMODE asc;


Select o_orderstatus, l_shipmode From tpch.lineitem natural join tpch.orders natural join tpch.customer natural join tpch.nation Where l_linenumber >= 4 and n_name LIKE 'IND%'  and l_extendedprice < 60000

/* Extracted Query: */
Select o_orderstatus as O_ORDERSTATUS,  as L_SHIPMODE
 From customer, lineitem, nation, orders 
 Where lineitem.orderkey = orders.orderkey
 and customer.custkey = orders.custkey
 and lineitem.l_linestatus = orders.o_orderstatus
 and customer.nationkey = nation.nationkey
 and orders.o_totalprice = 168982.73
 and orders.o_orderpriority = '5-LOW'
 and orders.o_clerk = 'Clerk#000000061'
 and orders.o_shippriority = 0
 and orders.o_comment = 'ost slyly around the blithely bold requests.'
 and lineitem.partkey = 2997
 and lineitem.suppkey = 4248
 and lineitem.l_linenumber = 6
 and lineitem.l_quantity = 19.0
 and lineitem.l_extendedprice = 36099.81
 and lineitem.l_discount = 0.04
 and lineitem.l_tax = 0.08
 and lineitem.l_returnflag = 'R'
 and lineitem.l_shipinstruct = 'NONE'
 and lineitem.l_shipmode = 'AIR'
 and lineitem.l_comment = 're. unusual frets after the sl'
 and customer.c_name = 'Customer#000002900'
 and customer.c_address = 'xeicQEyv6I'
 and customer.c_phone = '19-292-999-1038'
 and customer.c_acctbal = 4794.87
 and customer.c_mktsegment = 'HOUSEHOLD'
 and customer.c_comment = 'ironic packages. pending, regular deposits cajole blithely. carefully even instructions engage stealthily carefull'
 and nation.n_name = 'INDONESIA'
 and nation.regionkey = 2
 and nation.n_comment = 'slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull' 
 Order By L_SHIPMODE asc;


SELECT n_name, r_name FROM tpch.nation NATURAL JOIN tpch.region WHERE nationkey = 13

/* Extracted Query: */
Select  as N_NAME,  as R_NAME
 From nation, region 
 Where nation.regionkey = region.regionkey
 and nation.nationkey = 13
 and nation.n_name = 'ALGERIA'
 and nation.n_comment = 'embark quickly. bold foxes adapt slyly'
 and region.r_name = 'AFRICA'
 and region.r_comment = 'nag efully about the slyly bold instructions. quickly regular pinto beans wake blithely';


Select o_orderstatus, l_shipmode From tpch.orders natural join tpch.lineitem Where l_linenumber >= 6 

/* Extracted Query: */
Select o_orderstatus as O_ORDERSTATUS,  as L_SHIPMODE
 From lineitem, orders 
 Where lineitem.orderkey = orders.orderkey
 and lineitem.l_linestatus = orders.o_orderstatus
 and orders.custkey = 23014
 and orders.o_totalprice = 168982.73
 and orders.o_orderpriority = '5-LOW'
 and orders.o_clerk = 'Clerk#000000061'
 and orders.o_shippriority = 0
 and orders.o_comment = 'ost slyly around the blithely bold requests.'
 and lineitem.partkey = 2997
 and lineitem.suppkey = 4248
 and lineitem.l_linenumber = 6
 and lineitem.l_quantity = 19.0
 and lineitem.l_extendedprice = 36099.81
 and lineitem.l_discount = 0.04
 and lineitem.l_tax = 0.08
 and lineitem.l_returnflag = 'R'
 and lineitem.l_shipinstruct = 'NONE'
 and lineitem.l_shipmode = 'AIR'
 and lineitem.l_comment = 're. unusual frets after the sl' 
 Order By L_SHIPMODE asc;


SELECT n_name as name, c_acctbal as account_balance FROM tpch.orders NATURAL JOIN tpch.customer NATURAL JOIN tpch.nation WHERE o_totalprice <= 70000

/* Extracted Query: */
Select n_name as NAME, c_acctbal as ACCOUNT_BALANCE
 From customer, nation, orders 
 Where customer.custkey = orders.custkey
 and customer.nationkey = nation.nationkey;


SELECT count(n_name), r_name FROM tpch.nation NATURAL JOIN tpch.region WHERE nationkey = 13 group by r_name

/* Extracted Query: */
Select Count(*) as COUNT(N_NAME), r_name as R_NAME
 From nation, region 
 Where nation.regionkey = region.regionkey
 and nation.nationkey = 13 
 Group By r_name 
 Order By COUNT(N_NAME) desc;


