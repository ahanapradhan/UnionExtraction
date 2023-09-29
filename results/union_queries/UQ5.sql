SELECT o_orderkey, o_orderdate
FROM orders, customer where o_custkey = c_custkey and c_name like '%0001248%'
 AND o_orderdate >= '1997-01-01'
UNION ALL
SELECT l_orderkey, l_shipdate
FROM lineitem,
orders where l_orderkey = o_orderkey
and o_orderdate < '1994-01-01'
  AND l_quantity > 20
  AND l_extendedprice > 1000;