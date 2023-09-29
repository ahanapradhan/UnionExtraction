(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price
FROM orders, lineitem where o_orderkey = l_orderkey
and o_orderdate <= '1995-01-01'
GROUP BY o_clerk
ORDER BY total_price DESC
LIMIT 10)
UNION ALL
(SELECT n_name as name, SUM(s_acctbal) AS total_price
FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%'
GROUP BY n_name
ORDER BY n_name DESC);