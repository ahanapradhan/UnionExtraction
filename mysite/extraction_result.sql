
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select ps_COMMENT, sum(ps_supplycost * ps_availqty) as value From partsupp, supplier, nation         Where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By ps_COMMENT         Order by value desc Limit 100;
 --- extracted query:
  
 Select ps_comment, Sum(ps_availqty*ps_supplycost) as value 
 From nation, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'ARGENTINA' 
 Group By ps_comment 
 Order By value desc, ps_comment asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT o_orderdate, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_orderdate ORDER BY total_price DESC LIMIT 10;
 --- extracted query:
  
 Select o_orderdate, Sum(l_extendedprice) as total_price 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-01-01' 
 Group By o_orderdate 
 Order By total_price desc, o_orderdate desc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT o_orderdate, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_orderdate ORDER BY total_price DESC LIMIT 10;
 --- extracted query:
  
 Select o_orderdate, Sum(l_extendedprice) as total_price 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-01-01' 
 Group By o_orderdate 
 Order By total_price desc, o_orderdate desc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select o_orderpriority, count(*) as order_count From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate Group By o_orderpriority Order By o_orderpriority;
 --- extracted query:
  
 Select o_orderpriority, Count(*) as order_count 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_commitdate < lineitem.l_receiptdate
 and orders.o_orderdate between '1993-07-01' and '1993-09-30' 
 Group By o_orderpriority 
 Order By o_orderpriority asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select o_orderpriority, count(*) as order_count From orders, lineitem Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_commitdate < l_receiptdate Group By o_orderpriority Order By o_orderpriority;
 --- extracted query:
  
 Select o_orderpriority, Count(*) as order_count 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_commitdate < lineitem.l_receiptdate
 and orders.o_orderdate between '1993-07-01' and '1993-09-30' 
 Group By o_orderpriority 
 Order By o_orderpriority asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
  
 Select c_name, 121.65 as avg_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal = 121.65
 and orders.o_orderdate between '1993-10-15' and '1995-10-23' 
 Group By c_name, o_clerk 
 Order By c_name asc, avg_balance asc, o_clerk asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name, avg(c_acctbal) as avg_balance, o_clerk from customer, orders where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' and o_orderdate <= DATE '1995-10-23' and c_acctbal = 121.65 group by c_name, o_clerk order by c_name, o_clerk;
 --- extracted query:
  
 Select c_name, 121.65 as avg_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal = 121.65
 and orders.o_orderdate between '1993-10-15' and '1995-10-23' 
 Group By c_name, o_clerk 
 Order By c_name asc, avg_balance asc, o_clerk asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name as entity_name, n_name as country, o_totalprice as price
        from orders LEFT OUTER JOIN 
        customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
        o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
        where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
        group by n_name, c_name, o_totalprice
        order by price
        limit 10;
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, o_totalprice as price 
 From  customer 
 RIGHT OUTER JOIN  orders 
	 ON customer.c_custkey = orders.o_custkey
	 and orders.o_orderstatus = 'F'
	 and orders.o_totalprice <= customer.c_acctbal
 LEFT OUTER JOIN  nation 
	 ON customer.c_nationkey = nation.n_nationkey 
 Where orders.o_orderdate between '1994-01-01' and '1994-01-05' 
 Group By c_name, n_name, o_totalprice 
 Order By price asc, country asc, entity_name asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name as entity_name, n_name as country, o_totalprice as price
        from orders LEFT OUTER JOIN 
        customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
        o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
        where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
        group by n_name, c_name, o_totalprice
        order by price
        limit 10;
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, o_totalprice as price 
 From  customer 
 RIGHT OUTER JOIN  orders 
	 ON customer.c_custkey = orders.o_custkey
	 and orders.o_orderstatus = 'F'
	 and orders.o_totalprice <= customer.c_acctbal
 LEFT OUTER JOIN  nation 
	 ON customer.c_nationkey = nation.n_nationkey 
 Where orders.o_orderdate between '1994-01-01' and '1994-01-05' 
 Group By c_name, n_name, o_totalprice 
 Order By price asc, country asc, entity_name asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name as entity_name, n_name as country, o_totalprice as price
        from orders LEFT OUTER JOIN 
        customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
        o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
        where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
        group by n_name, c_name, o_totalprice
        order by price
        limit 10;
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, o_totalprice as price 
 From  customer 
 RIGHT OUTER JOIN  orders 
	 ON customer.c_custkey = orders.o_custkey
	 and orders.o_orderstatus = 'F'
	 and orders.o_totalprice <= customer.c_acctbal
 LEFT OUTER JOIN  nation 
	 ON customer.c_nationkey = nation.n_nationkey 
 Where orders.o_orderdate between '1994-01-01' and '1994-01-05' 
 Group By c_name, n_name, o_totalprice 
 Order By price asc, country asc, entity_name asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0)
 UNION ALL  
 (Select l_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0)
 UNION ALL  
 (Select l_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { part }, FROM(q2) = { customer }, FROM(q3) = { orders }, FROM(q4) = { lineitem }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { customer }, FROM(q2) = { part }, FROM(q3) = { orders }, FROM(q4) = { lineitem }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { customer }, FROM(q2) = { lineitem }, FROM(q3) = { part }, FROM(q4) = { orders }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select p_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name, (2.24*c_acctbal + 5.48*o_totalprice + 2.5*o_shippriority + 325) as max_balance, o_clerk
FROM customer, orders
where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
and o_orderdate <= DATE '1995-10-23' and
c_acctbal > 30.04;
 --- extracted query:
  
 Select c_name, 2.24*c_acctbal + 2.5*o_shippriority + 5.48*o_totalprice + 325 as max_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal >= 30.05
 and orders.o_orderdate between '1993-10-15' and '1995-10-23';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name as entity_name, n_name as country, o_totalprice as price
        from orders LEFT OUTER JOIN 
        customer on c_custkey = o_custkey and c_acctbal >= o_totalprice and
        o_orderstatus = 'F' LEFT OUTER JOIN nation ON c_nationkey = n_nationkey 
        where o_orderdate between DATE  '1994-01-01' and DATE '1994-01-05'
        group by n_name, c_name, o_totalprice
        order by price
        limit 10;
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, o_totalprice as price 
 From  customer 
 RIGHT OUTER JOIN  orders 
	 ON customer.c_custkey = orders.o_custkey
	 and orders.o_orderstatus = 'F'
	 and orders.o_totalprice <= customer.c_acctbal
 LEFT OUTER JOIN  nation 
	 ON customer.c_nationkey = nation.n_nationkey 
 Where orders.o_orderdate between '1994-01-01' and '1994-01-05' 
 Group By c_name, n_name, o_totalprice 
 Order By price asc, country asc, entity_name asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select o_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select l_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name, avg(2.24*c_acctbal + o_totalprice + 325.64) as max_balance, o_clerk 
                FROM customer, orders 
                where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
                and o_orderdate <= DATE '1995-10-23' and 
                c_acctbal > 0 and c_acctbal < 30.04
                group by c_name, o_clerk 
                order by c_name, o_clerk desc;
 --- extracted query:
  
 Select c_name, Avg(2.24*c_acctbal + o_totalprice + 325.64) as max_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal between 0.01 and 30.03
 and orders.o_orderdate between '1993-10-15' and '1995-10-23' 
 Group By c_name, o_clerk 
 Order By c_name asc, o_clerk desc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name, avg(2.24*c_acctbal + o_totalprice + 325.64) as max_balance, o_clerk 
                FROM customer, orders 
                where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14' 
                and o_orderdate <= DATE '1995-10-23' and 
                c_acctbal > 0 and c_acctbal < 30.04
                group by c_name, o_clerk 
                order by c_name, o_clerk desc;
 --- extracted query:
  
 Select c_name, Avg(2.24*c_acctbal + o_totalprice + 325.64) as max_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal between 0.01 and 30.03
 and orders.o_orderdate between '1993-10-15' and '1995-10-23' 
 Group By c_name, o_clerk 
 Order By c_name asc, o_clerk desc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select l_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select o_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0)
 UNION ALL  
 (Select p_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select l_orderkey as key 
 From lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select p_partkey as key 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 890.0);
 --- END OF ONE EXTRACTION EXPERIMENT
