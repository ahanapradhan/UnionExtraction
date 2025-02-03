Q1 = """select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        (select wl_returnflag as l_returnflag,
        wl_linestatus as l_linestatus,
        wl_quantity as l_quantity,
        wl_extendedprice as l_extendedprice,
        wl_discount as l_discount,
        wl_tax as l_tax 
        from web_lineitem where wl_shipdate <= date '1998-12-01' - interval '3' day
        UNION ALL 
        select sl_returnflag as l_returnflag,
        sl_linestatus as l_linestatus,
        sl_quantity as l_quantity,
        sl_extendedprice as l_extendedprice,
        sl_discount as l_discount,
        sl_tax as l_tax 
        from store_lineitem where sl_shipdate <= date '1998-12-01' - interval '3' day
        ) as lineitem
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,"""

Q2 = """
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey limit 100;"""

Q3 = """WITH combined_data AS (
    (SELECT
        wl_orderkey AS orderkey,
        wl_extendedprice * (1 - wl_discount) AS l_discounted_price,
        o_orderdate,
        o_shippriority
    FROM
        customer
    JOIN orders ON c_custkey = o_custkey
    JOIN web_lineitem ON wl_orderkey = o_orderkey
    WHERE
        c_mktsegment = 'FURNITURE'
        AND o_orderdate < DATE '1995-01-01'
        AND wl_shipdate > DATE '1995-01-01')
    
    UNION ALL
    
    (SELECT
        sl_orderkey AS orderkey,
        sl_extendedprice * (1 - sl_discount) AS l_discounted_price,
        o_orderdate,
        o_shippriority
    FROM
        customer
    JOIN orders ON c_custkey = o_custkey
    JOIN store_lineitem ON sl_orderkey = o_orderkey
    WHERE
        c_mktsegment = 'FURNITURE'
        AND o_orderdate < DATE '1995-01-01'
        AND sl_shipdate > DATE '1995-01-01'
    )
)
SELECT
    o_shippriority,
    SUM(l_discounted_price) AS revenue,
FROM
    combined_data GROUP BY
        orderkey, o_orderdate, o_shippriority
ORDER BY
    revenue DESC
LIMIT 10;"""
Q4 = """select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '3' month
        and exists (
                (select
                        *
                from
                        web_lineitem
                where
                        wl_orderkey = o_orderkey
                        and wl_commitdate < wl_receiptdate)
                UNION ALL
                (select
                        *
                from
                        store_lineitem
                where
                        sl_orderkey = o_orderkey
                        and sl_commitdate < sl_receiptdate)
        )
group by
        o_orderpriority
order by
        o_orderpriority;"""
Q5 = """select
        n_name,
        sum(los.l_extendedprice * (1 - los.l_discount)) as revenue
from
        customer,
        ( select 
            wl_extendedprice as l_extendedprice,
            wl_discount as l_discount,
            wl_suppkey as l_suppkey,
            wl_orderkey as l_orderkey,
            s_nationkey,
            o_custkey
        from web_lineitem, orders, supplier
        where 
            o_orderdate >= date '1995-01-01'
            and o_orderdate < date '1995-01-01' + interval '1' year
            and wl_orderkey = o_orderkey
            and wl_suppkey = s_suppkey
        UNION ALL
         select 
            sl_extendedprice as l_extendedprice,
            sl_discount as l_discount,
            sl_suppkey as l_suppkey,
            sl_orderkey as l_orderkey,
            s_nationkey,
            o_custkey
        from store_lineitem, orders, supplier
        where 
            o_orderdate >= date '1995-01-01'
            and o_orderdate < date '1995-01-01' + interval '1' year
            and sl_orderkey = o_orderkey
            and sl_suppkey = s_suppkey
        ) as los,
        nation,
        region
where
        c_custkey = los.o_custkey
        and c_nationkey = los.s_nationkey
        and los.s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
group by
        n_name
order by
        revenue desc;"""
Q6 = """select
        sum(lineitem.l_extendedprice * lineitem.l_discount) as revenue
from
        (select wl_extendedprice as l_extendedprice,
        wl_discount as l_discount
        from web_lineitem 
        where wl_shipdate >= date '1993-01-01'
        and wl_shipdate < date '1994-03-01' + interval '1' year
        and wl_discount between 0.06 - 0.01 and 0.06 + 0.01
        and wl_quantity < 10
        UNION ALL
        select sl_extendedprice as l_extendedprice,
        sl_discount as l_discount
        from store_lineitem 
        where sl_shipdate >= date '1993-01-01'
        and sl_shipdate < date '1994-03-01' + interval '1' year
        and sl_discount between 0.06 - 0.01 and 0.06 + 0.01
        and sl_quantity < 10) as lineitem;"""
Q7 = """"""
Q8 = """"""
Q9 = """"""
Q10 = """"""
Q11 = """"""
Q12 = """"""
Q13 = """"""
Q14 = """"""
Q15 = """"""
Q16 = """"""
Q17 = """"""
Q18 = """"""
Q19 = """"""
Q20 = """"""
Q21 = """"""
Q22 = """"""
Q23 = """"""
Q24 = """"""