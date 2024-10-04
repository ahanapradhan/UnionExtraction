-- UQ1
-- QH
(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100
Order By p_partkey Limit 5)
UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and
ps_availqty > 200 Order By s_suppkey Limit 7);
-- QE
 (Select s_suppkey as p_partkey, s_name as p_name
 From partsupp, supplier
 Where partsupp.ps_suppkey = supplier.s_suppkey
 and partsupp.ps_availqty >= 201
 Order By p_partkey asc
 Limit 7)
 UNION ALL
 (Select p_partkey, p_name
 From part, partsupp
 Where part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_availqty >= 101
 Order By p_partkey asc
 Limit 5);
 -- End of One Extraction

-- UQ2
-- QH
(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' order by s_suppkey desc, s_name limit 12)UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT' order by c_custkey, c_name desc limit 10);
-- QE
(Select c_custkey as s_suppkey, c_name as s_name
 From customer, orders
 Where customer.c_custkey = orders.o_custkey
 and orders.o_orderpriority = '1-URGENT'
 Order By s_suppkey asc, s_name desc
 Limit 10)
 UNION ALL
 (Select s_suppkey, s_name
 From nation, supplier
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'GERMANY'
 Order By s_suppkey desc, s_name asc
 Limit 12);
 -- End of One Extraction

 -- UQ3
-- QH
(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' Order by key Limit 10)
 UNION ALL (SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity > 35 Order By key Limit 10) 
 UNION ALL (select n_nationkey as key, r_name as name from nation, region where n_name LIKE 'B%' Order By key Limit 5)
-- QE
(Select n_nationkey as key, r_name as name
 From nation, region
 Where nation.n_name LIKE 'B%'
 Order By key asc
 Limit 5)
 UNION ALL
 (Select l_partkey as key, p_name as name
 From lineitem, part
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_quantity >= 35.01
 Order By key asc
 Limit 10)
 UNION ALL
 (Select c_custkey as key, c_name as name
 From customer, nation
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_name = 'UNITED STATES'
 Order By key asc
 Limit 10);
 -- End of One Extraction

 -- UQ4
-- QH
(SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' Order By c_custkey desc Limit 5) 
 UNION ALL (SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' Order By s_suppkey Limit 6) 
 UNION ALL (SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20 Order By p_partkey desc Limit 7) 
 UNION ALL (SELECT ps_partkey, p_name FROM part ,  partsupp where p_partkey = ps_partkey and ps_supplycost >= 1000 Order By ps_partkey Limit 8)
-- QE
 (Select s_suppkey as c_custkey, s_name as c_name
 From nation, supplier
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'CANADA'
 Order By c_custkey asc
 Limit 6)
 UNION ALL
 (Select p_partkey as c_custkey, p_name as c_name
 From lineitem, part
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_quantity >= 20.01
 Order By c_custkey desc
 Limit 7)
 UNION ALL
 (Select c_custkey, c_name
 From customer, nation
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_name = 'UNITED STATES'
 Order By c_custkey desc
 Limit 5)
 UNION ALL 
 (Select ps_partkey, p_name 
 From part, partsupp 
 Where part.p_partkey = partsupp.ps_partkey 
 and partsupp.ps_supplycost >= 1000.01 
 Order By ps_partkey asc);
 -- End of One Extraction

 -- UQ5
-- QH
(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' order by o_orderkey Limit 20) 
 UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);
-- QE
(Select o_orderkey, l_shipdate as o_orderdate, o_orderstatus as n_name
 From lineitem, orders
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1993-12-31'
 and lineitem.l_quantity >= 20.01
 and lineitem.l_extendedprice >= 1000.01
 Order By o_orderkey asc
 Limit 5)
 UNION ALL
 (Select o_orderkey, o_orderdate, n_name
 From customer, nation, orders
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_custkey = orders.o_custkey
 and customer.c_name LIKE '%0001248%'
 and orders.o_orderdate >= '1997-01-01'
 Order By o_orderkey asc
 Limit 20);
 -- End of One Extraction

 -- UQ6
-- QH
(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) 
 UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC Limit 10);
-- QE
Group By o_clerk
 Order By total_price desc, name desc
 Limit 10)
 UNION ALL
 (Select n_name as name, Sum(s_acctbal) as total_price
 From nation, supplier
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_name LIKE '%UNITED%'
 Group By n_name
 Order By name desc
 Limit 10);
 -- End of One Extraction

 -- UQ7
-- QH
(SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  Order By key Limit 20)
 UNION ALL  (SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost < 100 Order By price Limit 20);
-- QE
(Select p_partkey as key, p_retailprice as price, ps_suppkey as s_key
 From part, partsupp, supplier
 Where part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and partsupp.ps_supplycost <= 99.99
 Order By price asc
 Limit 20)
 UNION ALL
 (Select l_orderkey as key, l_extendedprice as price, l_partkey as s_key
 From lineitem
 Where lineitem.l_quantity >= 30.01
 and lineitem.l_shipdate between '1994-01-01' and '1994-12-31'
 Order By key asc
 Limit 20);
 -- End of One Extraction

 -- UQ8
-- QH
(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM
customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01'
GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL
(SELECT     l_orderkey as order_id,     AVG(l_quantity) AS total FROM     orders, lineitem where
l_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     l_orderkey ORDER BY
total DESC LIMIT 10);
-- QE
(Select o_custkey as order_id, Count(*) as total
 From customer, orders
 Where customer.c_custkey = orders.o_custkey
 and orders.o_orderdate >= '1995-01-01'
 Group By o_custkey
 Order By total asc, order_id desc
 Limit 10)
 UNION ALL
 (Select l_orderkey as order_id, Avg(l_quantity) as total
 From lineitem, orders
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1996-06-30'
 Group By l_orderkey
 Order By total desc
 Limit 10);
 -- End of One Extraction

 -- T5
-- QH
(select c_name, n_name from customer, nation where
c_mktsegment='BUILDING' and c_acctbal > 100 and c_nationkey
= n_nationkey) UNION ALL (select s_name, n_name from supplier,
nation where s_acctbal > 4000 and s_nationkey = n_nationkey)
-- QE
(Select c_name, n_name
 From customer, nation
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_mktsegment = 'BUILDING'
 and customer.c_acctbal >= 100.01)
 UNION ALL
 (Select s_name as c_name, n_name
 From nation, supplier
 Where nation.n_nationkey = supplier.s_nationkey
 and supplier.s_acctbal >= 4000.01);
 -- End of One Extraction











