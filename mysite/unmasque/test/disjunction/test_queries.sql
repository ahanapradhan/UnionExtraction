-- DQ1
  select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and 
  n_nationkey IN (1, 2, 3, 5, 4, 10, 13, 14, 15) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;

-- DQ2
select n_name, c_acctbal from nation, customer where n_nationkey = c_nationkey and c_nationkey > 3 and 
  n_nationkey < 20 and c_nationkey != 10 and c_acctbal < 7000 LIMIT 200;

-- DQ3
select sum(l_extendedprice) as revenue from lineitem 
  where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year 
  and (l_quantity < 10 or l_quantity between 24 and 42 or l_quantity >= 50);

-- DQ4
select avg(ps_supplycost) as cost from part, partsupp where p_partkey = ps_partkey 
  and (p_brand = 'Brand#52' or p_brand = 'Brand#12' or p_container = 'LG CASE');

-- DQ5
select p_size, ps_suppkey, count(*) as low_line_count from part. partsupp where 
  p_partkey = ps_partkey and p_brand IN ('Brand#52', 'Brand#34', 'Brand#15') and p_container IN ('WRAP BOX', 'MED BOX') 
  GROUP BY p_size, ps_suppkey  ORDER BY ps_suppkey desc LIMIT 30);

-- DQ6
select n_name, s_acctbal, ps_availqty  from supplier, partsupp, nation where 
  ps_suppkey=s_suppkey AND ps_supplycost < 50 and s_nationkey=n_nationkey and (n_regionkey = 1 or n_regionkey =3) ORDER BY n_name;

-- DQ7
select l_shipmode,sum(l_extendedprice) as revenue from lineitem 
  where l_shipdate >= date '1993-01-01' and l_shipdate < date '1994-01-01' + interval '1' year 
  and ((l_orderkey > 124 and l_orderkey < 135) or (l_orderkey > 235 and l_orderkey < 370) or (l_orderkey > 460)) 
  group by l_shipmode order by l_shipmode limit 100;

-- DQ8
select c_mktsegment as segment from customer, nation, orders, lineitem where c_acctbal between 9000 and 10000 
  and c_nationkey = n_nationkey and c_custkey = o_custkey and l_orderkey = o_orderkey 
  and (n_name = 'BRAZIL' or n_regionkey = 3);

-- DQ9
select n_name,SUM(s_acctbal) from supplier,partsupp,nation where ps_suppkey=s_suppkey and 
  s_nationkey=n_nationkey and (s_acctbal > 2000 or ps_supplycost < 500 or ps_supplycost >= 7000) group by n_name ORDER BY n_name LIMIT 10;


