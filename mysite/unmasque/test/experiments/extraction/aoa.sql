--AQ10
--QH
Select l_shipmode, count(*) as count From orders, lineitem Where
o_orderkey = l_orderkey and l_commitdate < l_receiptdate and
l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and
l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice
and l_extendedprice <= 70000 and o_totalprice > 60000 Group By
l_shipmode Order By l_shipmode;
--QE
Select l_shipmode, Count(*) as count
 From lineitem, orders
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_shipdate < lineitem.l_commitdate
 and lineitem.l_commitdate < lineitem.l_receiptdate
 and lineitem.l_extendedprice <= orders.o_totalprice
 and orders.o_totalprice >= 60000.01
 and lineitem.l_extendedprice <= 70000.0
 and lineitem.l_receiptdate between '1994-01-01' and '1994-12-31'
 Group By l_shipmode
 Order By l_shipmode asc;
  -- End of One Extraction

--AQ11
--QH
Select o_orderpriority, count(*) as order_count From orders, lineitem
Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and
o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group
By o_orderpriority Order By o_orderpriority;
--QE
 Select o_orderpriority, Count(*) as order_count
 From lineitem, orders
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_commitdate <= lineitem.l_receiptdate
 and orders.o_orderdate <= lineitem.l_receiptdate
 and orders.o_orderdate between '1993-07-01' and '1993-09-30'
 Group By o_orderpriority
 Order By o_orderpriority asc;
  -- End of One Extraction

--AQ13
--QH
Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where
o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey
= l_suppkey and ps_availqty = l_linenumber and l_shipdate >=
o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <=
l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;
--QE
 Select l_orderkey, l_linenumber
 From lineitem, orders, partsupp
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_partkey = partsupp.ps_partkey
 and lineitem.l_suppkey = partsupp.ps_suppkey
 and lineitem.l_linenumber = partsupp.ps_availqty
 and lineitem.l_shipdate <= lineitem.l_commitdate
 and orders.o_orderdate <= lineitem.l_shipdate
 and lineitem.l_commitdate <= lineitem.l_receiptdate
 and orders.o_orderdate >= '1990-01-01'
 and lineitem.l_receiptdate >= '1994-01-02'
 Order By l_orderkey asc
 Limit 7;
  -- End of One Extraction

--AQ14
--QH
 Select s_name, count(*) as numwait From supplier, lineitem, orders,
nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and
o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey
= n_nationkey Group By s_name Order By numwait desc Limit 100;
--QE
 (Select s_name, Count(*) as numwait
 From lineitem, nation, orders, supplier
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and lineitem.l_commitdate <= lineitem.l_receiptdate
 and orders.o_orderstatus = 'F'
 Group By s_name
 Order By numwait desc, s_name asc
 Limit 100);
 --End of One Extraction

--AQ15
--QH
Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price, sum(l_extendedprice
* (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 -
l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*)
as count_order From lineitem Where l_shipdate <= l_receiptdate and
l_receiptdate <= l_commitdate Group By l_returnflag, l_linestatus
Order by l_returnflag, l_linestatus;
--QE
 Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, Sum(l_extendedprice*(1 - l_discount)) as sum_disc_price, Sum(l_extendedprice*(-l_discount*l_tax - l_discount + l_tax + 1)) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, Avg(l_discount) as avg_disc, Count(*) as count_order
 From lineitem
 Where lineitem.l_shipdate <= lineitem.l_receiptdate
 and lineitem.l_receiptdate <= lineitem.l_commitdate
 Group By l_linestatus, l_returnflag
 Order By l_returnflag asc, l_linestatus asc;
 --End of One Extraction

--Q16
--QH
 Select p_brand, p_type, p_size, Count(*) as supplier_cnt
 From part, partsupp
 Where part.p_partkey = partsupp.ps_partkey
 and part.p_size >= 4 and part.p_type NOT LIKE 'SMALL PLATED%'  and part.p_brand <> 'Brand#45'
 Group By p_brand, p_size, p_type
 Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;
--QE
 Select p_brand, p_type, p_size, Count(*) as supplier_cnt
 From part, partsupp
 Where part.p_partkey = partsupp.ps_partkey
 and part.p_size >= 4
 and part.p_type NOT LIKE 'SMALL PLATED%'
 and part.p_brand <> 'Brand#45'
 Group By p_brand, p_size, p_type
 Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;
 --End of One Extraction



