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


--MQ7
--QH
select n_name, c_acctbal from nation LEFT OUTER JOIN customer ON n_nationkey = c_nationkey and c_nationkey > 3 and n_nationkey < 20 and c_nationkey != 10 and c_acctbal < 7000 LIMIT 200;
--QE
Select n_name, c_acctbal
 From  customer
 RIGHT OUTER JOIN  nation
         ON customer.c_nationkey = nation.n_nationkey
         and customer.c_nationkey between 4 and 19
         and customer.c_acctbal <= 6999.99
         and customer.c_nationkey <> 10
 Limit 200;
