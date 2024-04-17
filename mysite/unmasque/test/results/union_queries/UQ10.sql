Select l_shipmode, count(*) as count
From orders, lineitem
Where o_orderkey = l_orderkey
and l_commitdate < l_receiptdate
and l_shipdate < l_commitdate
and l_receiptdate >= '1994-01-01'
and l_receiptdate < '1995-01-01'
and l_extendedprice <= o_totalprice
and l_extendedprice <= 70000
and o_totalprice > 60000
Group By l_shipmode
Order By l_shipmode;