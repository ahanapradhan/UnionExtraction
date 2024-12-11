Q1 = """
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS total_quantity,
    AVG(l_quantity) AS avg_quantity,
    SUM(l_extendedprice) AS total_price,
    AVG(l_extendedprice) AS avg_price,
    SUM(l_discount) AS total_discount,
    AVG(l_discount) AS avg_discount,
    SUM(l_extendedprice * (1 - l_discount)) AS total_charge,
    AVG(l_extendedprice * (1 - l_discount)) AS avg_charge,
    COUNT(*) AS num_orders
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-11-28'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
"""

Q3 = """SELECT
    o_orderkey,
    o_orderdate,
    o_shippriority,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    orders
JOIN
    lineitem ON o_orderkey = l_orderkey
JOIN
    customer ON o_custkey = c_custkey
WHERE
    c_mktsegment = 'FURNITURE'
    AND o_orderdate < DATE '1995-01-01'
    AND l_shipdate > DATE '1995-01-01'
GROUP BY
    o_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC
LIMIT 1;
"""

Q4 = """SELECT
    o_orderpriority,
    COUNT(DISTINCT o_orderkey) AS num_orders
FROM
    orders
JOIN
    lineitem ON o_orderkey = l_orderkey
WHERE
    o_orderdate >= '1994-01-01'
    AND o_orderdate < '1994-04-01'
    AND l_commitdate < l_receiptdate
GROUP BY
    o_orderpriority;
"""

Q5 = """SELECT
    n.n_name AS nation,
    SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
    nation n ON c.c_nationkey = n.n_nationkey
JOIN
    region r ON n.n_regionkey = r.r_regionkey
WHERE
    r.r_name = 'ASIA'
    AND YEAR(o.o_orderdate) = 1995
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
"""

Q6 = """SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
FROM
    lineitem
WHERE
    l_shipdate BETWEEN DATE '1993-01-01' AND DATE '1994-02-28'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 10;
"""

Q9 = """SELECT
    n_name AS nation,
    EXTRACT(YEAR FROM o_orderdate) AS year,
    SUM((l_extendedprice * (1 - l_discount)) - ps_supplycost * l_quantity) AS annual_profit
FROM
    part
JOIN
    lineitem ON l_partkey = p_partkey
JOIN
    partsupp ON ps_partkey = p_partkey AND ps_suppkey = l_suppkey
JOIN
    orders ON o_orderkey = l_orderkey
JOIN
    supplier ON s_suppkey = l_suppkey
JOIN
    nation ON s_nationkey = n_nationkey
WHERE
    p_name LIKE 'co%'
GROUP BY
    nation,
    year
ORDER BY
    nation,
    year;
"""

Q10 = """SELECT
    n.n_name AS nation,
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    c.c_comment,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer c
JOIN
    orders o ON c.c_custkey = o.o_custkey
JOIN
    lineitem l ON o.o_orderkey = l.l_orderkey
JOIN
    nation n ON c.c_nationkey = n.n_nationkey
WHERE
    l.l_returnflag = 'R'
    AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1995-03-31'
GROUP BY
    n.n_name,
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    c.c_phone,
    c.c_comment
ORDER BY
    revenue DESC;
"""

Q12 = """SELECT
    CASE
        WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 'HIGH'
        ELSE 'LOW'
    END AS priority_category,
    COUNT(*) AS lineitem_count
FROM
    lineitem
JOIN
    orders ON l_orderkey = o_orderkey
WHERE
    l_shipmode = 'SHIP'
    AND YEAR(l_shipdate) = 1995
    AND l_shipdate < l_commitdate
    AND l_commitdate < l_receiptdate
    AND YEAR(l_receiptdate) = 1995
GROUP BY
    priority_category;
"""

Q14 = """SELECT
    100.0 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
    SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue_percentage
FROM
    lineitem
JOIN
    part ON l_partkey = p_partkey
WHERE
    l_shipdate >= '1995-01-01'
    AND l_shipdate < '1995-02-01';
"""

Q15 = """SELECT
    s.s_name,
    s.s_address,
    s.s_phone,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
FROM
    supplier s
JOIN
    lineitem l ON s.s_suppkey = l.l_suppkey
WHERE
    l.l_shipdate >= '1995-01-01'
    AND l.l_shipdate < '1995-04-01'
GROUP BY
    s.s_suppkey,
    s.s_name,
    s.s_address,
    s.s_phone
ORDER BY
    total_revenue DESC
LIMIT 1;
"""