(Select p_brand, o_clerk, l_shipmode
 From lineitem, orders, part 
 Where l_partkey = p_partkey
 and l_orderkey = o_orderkey
 and p_container = 'LG CAN'
 and l_extendedprice <= p_retailprice
 and o_orderdate <= l_shipdate
 and '1994-01-02' <= o_orderdate
 and '1995-01-02' <= l_shipdate
 and l_suppkey <= 9999
 and p_partkey <= 9999 
 Limit 8)
 UNION ALL 
(Select p_brand, s_name as o_clerk, l_shipmode
 From lineitem, part, supplier 
 Where l_partkey = p_partkey
 and p_container = 'LG CAN'
 and l_shipdate  >= '1995-01-02'
 and l_suppkey <= 13999
 and l_partkey <= 14999
 and l_extendedprice <= s_acctbal 
 Limit 10);