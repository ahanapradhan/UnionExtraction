Select l_orderkey, l_linenumber
From orders, lineitem, partsupp
Where ps_partkey = l_partkey and ps_suppkey = l_suppkey
and o_orderkey = l_orderkey and
l_shipdate >= o_orderdate and ps_availqty <= l_linenumber
Order By l_orderkey LIMIT 10;