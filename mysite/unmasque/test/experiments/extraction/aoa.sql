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
--QE
 --End of One Extraction

--AQ15
--QH
--QE
 --End of One Extraction

--Q16
--QH
--QE
 --End of One Extraction



