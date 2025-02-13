```sql
SELECT
    orderkey,
    SUM(extendedprice * (1 - discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM (
    SELECT
        wl_orderkey AS orderkey,
        wl_extendedprice AS extendedprice,
        wl_discount AS discount,
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
    UNION ALL
    SELECT
        sl_orderkey AS orderkey,
        sl_extendedprice AS extendedprice,
        sl_discount AS discount,
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
) AS combined_orders
GROUP BY
    orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC
LIMIT 10;
```

-- Prompt Token count = 1324

```sql
(select
        orderkey,
        sum(extendedprice * (1 - discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        (select
                wl_orderkey as orderkey,
                wl_extendedprice as extendedprice,
                wl_discount as discount,
                o_orderdate,
                o_shippriority
        from
                customer,
                orders,
                web_lineitem
        where
                c_mktsegment = 'FURNITURE'
                and c_custkey = o_custkey
                and wl_orderkey = o_orderkey
                and o_orderdate < date '1995-01-01'
                and wl_shipdate > date '1995-01-01'
        union all
        select
                sl_orderkey as orderkey,
                sl_extendedprice as extendedprice,
                sl_discount as discount,
                o_orderdate,
                o_shippriority
        from
                customer,
                orders,
                web_lineitem
        where
                c_mktsegment = 'FURNITURE'
                and c_custkey = o_custkey
                and sl_orderkey = o_orderkey
                and o_orderdate < date '1995-01-01'
                and sl_shipdate > date '1995-01-01') as combined
group by
        orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc
limit 10)
```

-- Prompt Token count = 1333

