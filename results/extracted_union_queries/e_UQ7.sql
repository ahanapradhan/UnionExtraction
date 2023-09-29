(Select p_partkey as key, p_retailprice as price, s_suppkey as s_key
From part, supplier, partsupp
Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and ps_supplycost  <= 99.0)
 UNION ALL 
(Select l_orderkey as key, l_extendedprice as price, l_partkey as s_key
From lineitem
Where l_quantity  >= 30.5 and l_shipdate  >= '1994-01-02' and l_shipdate <= '1994-12-31');