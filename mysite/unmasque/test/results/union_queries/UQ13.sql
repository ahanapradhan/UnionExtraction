Select l_orderkey, l_linenumber
From orders, lineitem, partsupp
Where o_orderkey = l_orderkey
and ps_partkey = l_partkey and ps_suppkey = l_suppkey and
ps_availqty = l_linenumber and l_shipdate >= o_orderdate and o_orderdate >= '1990-01-01'
and
l_commitdate <= l_receiptdate and l_shipdate <= l_commitdate
and l_receiptdate > '1994-01-01'
Order By l_orderkey
Limit 7;