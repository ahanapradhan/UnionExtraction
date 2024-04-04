SELECT n_name as name, c_acctbal as account_balance FROM tpch.orders NATURAL JOIN tpch.customer NATURAL JOIN tpch.nation WHERE o_totalprice <= c_acctbal

/* Extracted WHERE Clause: */
customer.custkey = orders.custkey
 and customer.nationkey = nation.nationkey
 and orders.o_totalprice <= customer.c_acctbal


Select o_orderstatus, l_shipmode From tpch.lineitem natural join tpch.orders Where l_linenumber >= 4 

/* Extracted WHERE Clause: */
lineitem.orderkey = orders.orderkey
 and lineitem.l_linenumber  >= 4


Select o_orderstatus, l_shipmode From tpch.lineitem natural join tpch.orders natural join tpch.customer natural join tpch.nation Where l_linenumber >= 4 and n_name LIKE 'IND%'  and l_extendedprice < o_totalprice

/* Extracted WHERE Clause: */
lineitem.orderkey = orders.orderkey
 and customer.custkey = orders.custkey
 and customer.nationkey = nation.nationkey
 and nation.n_name LIKE 'IND%'
 and 4 <= lineitem.l_linenumber
 and lineitem.l_extendedprice < orders.o_totalprice


SELECT n_name, r_name FROM tpch.nation NATURAL JOIN tpch.region WHERE nationkey = 13

/* Extracted WHERE Clause: */
nation.regionkey = region.regionkey
 and nation.nationkey = 13


Select o_orderstatus, l_shipmode From tpch.orders natural join tpch.lineitem Where l_linenumber >= 6 

/* Extracted WHERE Clause: */
lineitem.orderkey = orders.orderkey
 and lineitem.l_linenumber  >= 6


