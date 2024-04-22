(Select p_brand, o_clerk, l_shipmode
From orders, lineitem, part
Where
l_partkey = p_partkey
and o_orderkey = l_orderkey
and l_shipdate >= o_orderdate
and o_orderdate > '1994-01-01'
and l_shipdate > '1995-01-01'
and p_retailprice >= l_extendedprice
and p_partkey < 10000
and l_suppkey < 10000
and p_container = 'LG CAN'
Order By o_clerk LIMIT 5)

UNION ALL

(Select p_brand, s_name, l_shipmode
From lineitem, part, supplier
Where
l_partkey = p_partkey
and s_suppkey = s_suppkey
and l_shipdate > '1995-01-01'
and s_acctbal >= l_extendedprice
and p_partkey < 15000
and l_suppkey < 14000
and p_container = 'LG CAN'
Order By s_name LIMIT 10);
