
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
