
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
