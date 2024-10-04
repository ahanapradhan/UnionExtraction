-- MQ9
--QH
(SELECT c_name as name, c_acctbal as account_balance FROM orders,
customer, nation WHERE c_custkey = o_custkey and c_nationkey
= n_nationkey and c_mktsegment = 'FURNITURE' and n_name =
'INDIA' and o_orderdate between '1998-01-01' and '1998-12-05' and
o_totalprice <= c_acctbal) UNION ALL (SELECT s_name as name,
s_acctbal as account_balance FROM supplier, lineitem, orders, nation
WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
and s_nationkey = n_nationkey and n_name = 'ARGENTINA' and
o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice >
s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal Order by account_balance desc limit 20);
--QE
(Select s_name as name, s_acctbal as account_balance
 From lineitem, nation, orders, supplier
 Where lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and supplier.s_acctbal < orders.o_totalprice
 and nation.n_name = 'ARGENTINA'
 and orders.o_orderdate between '1998-01-01' and '1998-01-05'
 and orders.o_totalprice >= 30000.0
 and supplier.s_acctbal <= 50000.0
 Order By account_balance desc
 Limit 20)
 UNION ALL
 (Select c_name as name, c_acctbal as account_balance
 From customer, nation, orders
 Where customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation.n_nationkey
 and orders.o_totalprice <= customer.c_acctbal
 and customer.c_mktsegment = 'FURNITURE'
 and nation.n_name = 'INDIA'
 and orders.o_orderdate between '1998-01-01' and '1998-12-05');
-- End of one Extraction


-- MQ12
--QH
(Select p_brand, o_clerk, l_shipmode From orders, lineitem, part Where
l_partkey = p_partkey and o_orderkey = l_orderkey and l_shipdate >=
o_orderdate and o_orderdate > '1994-01-01' and l_shipdate > '1995-01-01' and p_retailprice >= l_extendedprice and p_partkey < 10000 and
l_suppkey < 10000 and p_container = 'LG CAN' Order By o_clerk LIMIT
5) UNION ALL (Select p_brand, s_name, l_shipmode From lineitem,
part, supplier Where l_partkey = p_partkey and s_suppkey = s_suppkey
and l_shipdate > '1995-01-01' and s_acctbal >= l_extendedprice and
p_partkey < 15000 and l_suppkey < 14000 and p_container = 'LG CAN'
Order By s_name LIMIT 10);
-- QE
(Select p_brand, o_clerk, l_shipmode
 From lineitem, orders, part
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_extendedprice <= part.p_retailprice
 and orders.o_orderdate <= lineitem.l_shipdate
 and part.p_container = 'LG CAN'
 and part.p_partkey <= 9999
 and orders.o_orderdate >= '1994-01-02'
 and lineitem.l_shipdate >= '1995-01-02'
 and lineitem.l_suppkey <= 9999
 Order By o_clerk asc
 Limit 5)
 UNION ALL
 (Select p_brand, s_name as o_clerk, l_shipmode
 From lineitem, part, supplier
 Where lineitem.l_partkey = part.p_partkey
 and lineitem.l_extendedprice <= supplier.s_acctbal
 and part.p_container = 'LG CAN'
 and lineitem.l_shipdate >= '1995-01-02'
 and part.p_partkey <= 14999
 and lineitem.l_suppkey <= 13999
 Order By o_clerk asc
 Limit 10);
 --End of One Extraction

 --MQ2
 --QH
 (
	select l_orderkey, l_extendedprice as price, p_partkey from lineitem, part
	where l_partkey = p_partkey  and p_container LIKE 'JUMBO%' and p_partkey > 3000 and l_partkey < 3010
	Order by l_orderkey, price desc Limit 100
) union all (select o_orderkey, c_acctbal as price, c_custkey
from customer LEFT OUTER JOIN orders on c_custkey = o_custkey
 where c_custkey > 1000 and c_custkey < 1010 Order By price desc, o_orderkey, c_custkey Limit 10);
 --QE
 (Select o_orderkey as l_orderkey, c_acctbal as price, c_custkey as p_partkey
 From  customer
 LEFT OUTER JOIN  orders
         ON customer.c_custkey = orders.o_custkey
 Where customer.c_custkey between 1001 and 1009
 Order By price desc, l_orderkey asc, p_partkey asc
 Limit 10)
 UNION ALL
 (Select l_orderkey, l_extendedprice as price, l_partkey as p_partkey
 From lineitem, part
 Where lineitem.l_partkey = part.p_partkey
 and part.p_container LIKE 'JUMBO%'
 and lineitem.l_partkey between 3001 and 3009
 Order By l_orderkey asc, price desc
 Limit 100);

