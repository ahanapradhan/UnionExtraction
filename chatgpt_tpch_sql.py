```sql
SELECT
    returnflag,
    linestatus,
    SUM(sum_qty) AS sum_qty,
    SUM(sum_base_price) AS sum_base_price,
    SUM(sum_disc_price) AS sum_disc_price,
    SUM(sum_charge) AS sum_charge,
    AVG(avg_qty) AS avg_qty,
    AVG(avg_price) AS avg_price,
    AVG(avg_disc) AS avg_disc,
    SUM(count_order) AS count_order
FROM (
    SELECT
        wl_returnflag AS returnflag,
        wl_linestatus AS linestatus,
        SUM(wl_quantity) AS sum_qty,
        SUM(wl_extendedprice) AS sum_base_price,
        SUM(wl_extendedprice * (1 - wl_discount)) AS sum_disc_price,
        SUM(wl_extendedprice * (1 - wl_discount) * (1 + wl_tax)) AS sum_charge,
        AVG(wl_quantity) AS avg_qty,
        AVG(wl_extendedprice) AS avg_price,
        AVG(wl_discount) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        web_lineitem
    WHERE
        wl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
    GROUP BY
        wl_returnflag,
        wl_linestatus

    UNION ALL

    SELECT
        sl_returnflag AS returnflag,
        sl_linestatus AS linestatus,
        SUM(sl_quantity) AS sum_qty,
        SUM(sl_extendedprice) AS sum_base_price,
        SUM(sl_extendedprice * (1 - sl_discount)) AS sum_disc_price,
        SUM(sl_extendedprice * (1 - sl_discount) * (1 + sl_tax)) AS sum_charge,
        AVG(sl_quantity) AS avg_qty,
        AVG(sl_extendedprice) AS avg_price,
        AVG(sl_discount) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        store_lineitem
    WHERE
        sl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
    GROUP BY
        sl_returnflag,
        sl_linestatus
) AS combined
GROUP BY
    returnflag,
    linestatus
ORDER BY
    returnflag,
    linestatus;
```
