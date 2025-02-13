```sql
SELECT
    returnflag,
    linestatus,
    SUM(quantity) AS sum_qty,
    SUM(extendedprice) AS sum_base_price,
    SUM(extendedprice * (1 - discount)) AS sum_disc_price,
    SUM(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
    AVG(quantity) AS avg_qty,
    AVG(extendedprice) AS avg_price,
    AVG(discount) AS avg_disc,
    COUNT(*) AS count_order
FROM (
    SELECT
        wl_returnflag AS returnflag,
        wl_linestatus AS linestatus,
        wl_quantity AS quantity,
        wl_extendedprice AS extendedprice,
        wl_discount AS discount,
        wl_tax AS tax
    FROM web_lineitem
    WHERE wl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
    UNION ALL
    SELECT
        sl_returnflag AS returnflag,
        sl_linestatus AS linestatus,
        sl_quantity AS quantity,
        sl_extendedprice AS extendedprice,
        sl_discount AS discount,
        sl_tax AS tax
    FROM store_lineitem
    WHERE sl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
) AS combined
GROUP BY
    returnflag,
    linestatus
ORDER BY
    returnflag,
    linestatus;
```

-- Prompt Token count = 2862

