SELECT
    l_orderkey as key,
    l_extendedprice as price,
    l_partkey as s_key
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1995-01-01'
    AND l_quantity > 30

UNION ALL

SELECT
    ps_partkey as key,
    p_retailprice as price,
    ps_suppkey as s_key
FROM
    partsupp,supplier,part where ps_suppkey = s_suppkey
and ps_partkey = p_partkey
    AND ps_supplycost < 100;