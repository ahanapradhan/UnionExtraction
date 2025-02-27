```sql
SELECT
    wl_orderkey,
    SUM(wl_extendedprice * (1 - wl_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    web_lineitem
WHERE
    c_mktsegment = 'FURNITURE'
    AND c_custkey = o_custkey
    AND wl_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-01-01'
    AND wl_shipdate > DATE '1995-01-01'
GROUP BY
    wl_orderkey,
    o_orderdate,
    o_shippriority
UNION ALL
SELECT
    sl_orderkey,
    SUM(sl_extendedprice * (1 - sl_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    store_lineitem
WHERE
    c_mktsegment = 'FURNITURE'
    AND c_custkey = o_custkey
    AND sl_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-01-01'
    AND sl_shipdate > DATE '1995-01-01'
GROUP BY
    sl_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC
LIMIT 10;
```

-- Prompt Token count = 2768

```sql
SELECT
    orderkey,
    SUM(revenue) AS revenue,
    o_orderdate,
    o_shippriority
FROM (
    SELECT
        wl_orderkey AS orderkey,
        SUM(wl_extendedprice * (1 - wl_discount)) AS revenue,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        web_lineitem
    WHERE
        c_mktsegment = 'FURNITURE'
        AND c_custkey = o_custkey
        AND wl_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-01-01'
        AND wl_shipdate > DATE '1995-01-01'
    GROUP BY
        wl_orderkey,
        o_orderdate,
        o_shippriority

    UNION ALL

    SELECT
        sl_orderkey AS orderkey,
        SUM(sl_extendedprice * (1 - sl_discount)) AS revenue,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        store_lineitem
    WHERE
        c_mktsegment = 'FURNITURE'
        AND c_custkey = o_custkey
        AND sl_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-01-01'
        AND sl_shipdate > DATE '1995-01-01'
    GROUP BY
        sl_orderkey,
        o_orderdate,
        o_shippriority
) AS combined_orders
GROUP BY
    orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
```

-- Prompt Token count = 2777

