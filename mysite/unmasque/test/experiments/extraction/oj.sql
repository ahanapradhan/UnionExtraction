--OQ1
--QH
 select c_name, n_name, count(*) as total from nation RIGHT OUTER
JOIN customer ON c_nationkey = n_nationkey and c_acctbal < 1000
        GROUP BY c_name,
n_name Order by c_name, n_name desc Limit 10;
--QE
Select c_name, n_name, Count(*) as total
 From  customer
 LEFT OUTER JOIN  nation
         ON customer.c_nationkey = nation.n_nationkey
         and customer.c_acctbal <= 999.99
 Group By c_name, n_name
 Order By c_name asc, n_name desc
 Limit 10;
  -- End of One Extraction

  --OQ3
--QH
SELECT l_shipmode, o_shippriority ,count(*) as low_line_count FROM
lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND
o_totalprice > 50000 ) WHERE l_linenumber = 4 AND l_quantity < 30
GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;
--QE
 Select l_shipmode, o_shippriority, Count(*) as low_line_count
 From  lineitem
 LEFT OUTER JOIN  orders
         ON lineitem.l_orderkey = orders.o_orderkey
         and orders.o_totalprice >= 50000.01
 Where lineitem.l_quantity <= 29.99 and lineitem.l_linenumber = 4
 Group By l_shipmode, o_shippriority
 Order By l_shipmode asc, o_shippriority asc
 Limit 5;
  -- End of One Extraction

  --OQ4
--QH
SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name from orders FULL OUTER JOIN customer on c_custkey = o_custkey and
o_orderstatus = 'F' group by o_custkey, o_clerk, c_name order by key
limit 35;
--QE
Select o_custkey as key, Sum(c_acctbal) as sum, o_clerk, c_name
 From  customer
 FULL OUTER JOIN  orders
         ON customer.c_custkey = orders.o_custkey
         and orders.o_orderstatus = 'F'
 Group By o_custkey, c_name, o_clerk
 Order By key asc, o_clerk asc, c_name asc
 Limit 35;
  -- End of One Extraction

  --OQ5
--QH
SELECT p_size, s_phone, ps_supplycost, n_name FROM part RIGHT
OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size >
7 LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND
s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey =
n_nationkey and n_regionkey > 3 Order by ps_supplycost asc Limit 50;
--QE
Select p_size, s_phone, ps_supplycost, n_name
 From  part
 RIGHT OUTER JOIN  partsupp
         ON part.p_partkey = partsupp.ps_partkey
         and part.p_size >= 8
 LEFT OUTER JOIN  supplier
         ON partsupp.ps_suppkey = supplier.s_suppkey
         and supplier.s_acctbal <= 1999.99
 FULL OUTER JOIN  nation
         ON supplier.s_nationkey = nation.n_nationkey
         and nation.n_regionkey >= 4
 Order By ps_supplycost asc
 Limit 50;
  -- End of One Extraction

  --OQ6
  --QH
  Select ps_suppkey, p_name, p_type from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size > 4 and ps_availqty > 3350 Order By ps_suppkey Limit 10;
--QE
 Select ps_suppkey, p_name, p_type
 From  part
 RIGHT OUTER JOIN  partsupp
         ON part.p_partkey = partsupp.ps_partkey
         and part.p_size >= 5
         and partsupp.ps_availqty >= 3351
 Order By ps_suppkey asc
 Limit 10;
  -- End of One Extraction

--OQ7
--QH
SELECT p_name, s_phone, ps_supplycost, n_name FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7
	LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey = n_nationkey
	and n_regionkey > 3 Order By p_name, s_phone, ps_supplycost, n_name desc Limit 20;
--QE
 Select p_name, s_phone, ps_supplycost, n_name
 From  part
 RIGHT OUTER JOIN  partsupp
         ON part.p_partkey = partsupp.ps_partkey
         and part.p_size >= 8
 LEFT OUTER JOIN  supplier
         ON partsupp.ps_suppkey = supplier.s_suppkey
         and supplier.s_acctbal <= 1999.99
 FULL OUTER JOIN  nation
         ON supplier.s_nationkey = nation.n_nationkey
         and nation.n_regionkey >= 4
 Order By p_name asc, s_phone asc, ps_supplycost asc, n_name desc
 Limit 20;
  -- End of One Extraction
