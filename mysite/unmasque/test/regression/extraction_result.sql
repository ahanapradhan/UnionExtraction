
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name from orders FULL OUTER JOIN customer on c_custkey = o_custkey and o_orderstatus = 'F' group by o_custkey, o_clerk, c_name order by key limit 35;
 --- extracted query:
  
 Select o_custkey as key, Sum(c_acctbal) as sum, o_clerk, c_name 
 From  customer 
 FULL OUTER JOIN  orders 
	 ON customer.c_custkey = orders.o_custkey
	 and orders.o_orderstatus = 'F' 
 Group By o_custkey, c_name, o_clerk 
 Order By key asc, o_clerk asc, c_name asc 
 Limit 35;
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
 Order By total_price desc, o_orderdate asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_shipmode, sum(l_extendedprice) as revenue From lineitem Where l_shipdate  < '1994-01-01' and l_quantity < 24 and l_linenumber <> 4 and l_returnflag <> 'R' Group By l_shipmode Limit 100; 
 --- extracted query:
 
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_shipmode, sum(l_extendedprice) as revenue From lineitem Where l_shipdate  < '1994-01-01' and l_quantity < 24 and l_linenumber <> 4 and l_returnflag <> 'R' Group By l_shipmode Limit 100; 
 --- extracted query:
  
 (Select l_shipmode, Sum(l_extendedprice) as revenue 
 From lineitem 
 Where lineitem.l_quantity <= 23.99
 and lineitem.l_shipdate <= '1993-12-31' 
 Group By l_shipmode 
 Order By l_shipmode desc 
 Limit 100);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_shipmode, sum(l_extendedprice) as revenue From lineitem Where l_shipdate  < '1994-01-01' and l_quantity < 24 and l_linenumber <> 4 and l_returnflag <> 'R' Group By l_shipmode Limit 100; 
 --- extracted query:
  
 (Select l_shipmode, Sum(l_extendedprice) as revenue 
 From lineitem 
 Where lineitem.l_quantity <= 23.99
 and lineitem.l_shipdate <= '1993-12-31' 
 Group By l_shipmode 
 Order By l_shipmode desc 
 Limit 100);
 --- END OF ONE EXTRACTION EXPERIMENT
