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
        sum(lineitem.l_extendedprice *(1 - lineitem.l_discount)) as revenue
from
        (select wl_extendedprice as l_extendedprice,
        wl_discount as l_discount
        from web_lineitem 
        where wl_shipdate >= date '1993-01-01'
        and wl_shipdate < date '1994-03-01' + interval '1' year
        and wl_discount between 0.07 - 0.01 and 0.07 + 0.01
        and wl_quantity < 24
        UNION ALL
        select sl_extendedprice as l_extendedprice,
        sl_discount as l_discount
        from store_lineitem 
        where sl_shipdate >= date '1993-01-01'
        and sl_shipdate < date '1994-03-01' + interval '1' year
        and sl_discount between 0.07 - 0.01 and 0.07 + 0.01
        and sl_quantity < 24
        ) as lineitem;"""
Q7 = """SELECT supp_nation,
       cust_nation,
       l_year,
       SUM(volume) AS revenue
FROM   (SELECT n1.n_name  AS supp_nation,
               n2.n1_name  AS cust_nation,
               los.l_year AS l_year,
               los.volume AS volume
        FROM   (SELECT Extract(year FROM wl_shipdate)         AS l_year,
                       wl_extendedprice * ( 1 - wl_discount ) AS volume,
                       s_nationkey,
                       o_custkey
                FROM   supplier,
                       web_lineitem,
                       orders
                WHERE  s_suppkey = wl_suppkey
                       AND o_orderkey = wl_orderkey
                       AND wl_shipdate BETWEEN DATE '1995-01-01' AND DATE
                                               '1996-12-31'
                UNION ALL
                SELECT Extract(year FROM sl_shipdate)         AS l_year,
                       sl_extendedprice * ( 1 - sl_discount ) AS volume,
                       s_nationkey,
                       o_custkey
                FROM   supplier,
                       store_lineitem,
                       orders
                WHERE  s_suppkey = sl_suppkey
                       AND o_orderkey = sl_orderkey
                       AND sl_shipdate BETWEEN DATE '1995-01-01' AND DATE
                                               '1996-12-31')
               AS los,
               customer,
               nation n1,
               nation1 n2
        WHERE  c_custkey = los.o_custkey
               AND los.s_nationkey = n1.n_nationkey
               AND c_nationkey = n2.n1_nationkey
               AND ( ( n1.n_name = 'GERMANY'
                       AND n2.n1_name = 'FRANCE' )
                      OR ( n1.n_name = 'FRANCE'
                           AND n2.n1_name = 'GERMANY' ) )) AS shipping
GROUP  BY supp_nation,
          cust_nation,
          l_year
ORDER  BY supp_nation,
          cust_nation,
          l_year; """
Q8 = """"""
Q9 = """select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation, p_name,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        (select 
                        wl_extendedprice as l_extendedprice,
                        wl_discount as l_discount,
                        wl_quantity as l_quantity,
                        wl_suppkey as l_suppkey,
                        wl_partkey as l_partkey,
                        wl_orderkey as l_orderkey
                        from web_lineitem
                        UNION ALL
                        select 
                        sl_extendedprice as l_extendedprice,
                        sl_discount as l_discount,
                        sl_quantity as l_quantity,
                        sl_suppkey as l_suppkey,
                        sl_partkey as l_partkey,
                        sl_orderkey as l_orderkey
                        from store_lineitem
                        ) as lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like 'co%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc;"""
Q10 = """select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        (select 
                        wl_extendedprice as l_extendedprice,
                        wl_discount as l_discount,
                        wl_returnflag as l_returnflag,
                        wl_orderkey as l_orderkey
                        from web_lineitem
                        UNION ALL
                        select 
                        sl_extendedprice as l_extendedprice,
                        sl_discount as l_discount,
                        sl_returnflag as l_returnflag,
                        sl_orderkey as l_orderkey
                        from store_lineitem
                        ) as lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1995-01-01'
        and o_orderdate < date '1995-01-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc;"""
Q11 = """"""
Q12 = """"""
Q13 = """"""
Q14 = """select
        100.00 * sum(case
                when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
        (select 
		 sl_extendedprice as l_extendedprice,
		 sl_discount as l_discount,
		 sl_partkey as l_partkey,
		 sl_shipdate as l_shipdate
		 from store_lineitem
		 UNION ALL
		 select
		 wl_extendedprice as l_extendedprice,
		 wl_discount as l_discount,
		 wl_partkey as l_partkey,
		 wl_shipdate as l_shipdate
		 from web_lineitem
        ) as lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1995-01-01' + interval '1' month;"""
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