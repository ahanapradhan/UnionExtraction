(SELECT
    c_custkey as order_id,
    COUNT(*) AS total
FROM
    customer, orders where c_custkey = o_custkey
and
    o_orderdate >= '1995-01-01'
GROUP BY
    c_custkey
ORDER BY
    total ASC
LIMIT 10)
UNION ALL
(SELECT
    l_orderkey as order_id,
    AVG(l_quantity) AS total
FROM
    orders, lineitem where l_orderkey = o_orderkey
    AND o_orderdate < DATE '1996-07-01'
GROUP BY
    l_orderkey
ORDER BY
    total DESC
LIMIT 10);