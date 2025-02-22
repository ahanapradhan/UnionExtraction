import signal
import sys

from .core.factory.PipeLineFactory import PipeLineFactory
from .pipeline.abstract.TpchSanitizer import TpchSanitizer
from .util.ConnectionFactory import ConnectionHelperFactory
from .util.workload_queries import TestQuery


def signal_handler(signum, frame):
    print('You pressed Ctrl+C!')
    sigconn = ConnectionHelperFactory().createConnectionHelper()
    sigconn.connectUsingParams()
    sanitizer = TpchSanitizer(sigconn)
    sanitizer.sanitize()
    sigconn.closeConnection()
    print("database restored!")
    sys.exit(0)


class TestQuery:
    def __init__(self, name: str, hidden_query: str, cs2: bool, union: bool, oj: bool, nep: bool, orf=None):
        self.qid = name
        self.cs2 = cs2
        self.union = union
        self.oj = oj
        self.nep = nep
        self.query = hidden_query
        self.orf = orf if orf is not None else False


def create_workload():
    test_workload = [TestQuery("U1", """(SELECT p_partkey, p_name FROM part, partsupp where p_partkey = ps_partkey and ps_availqty > 100
Order By p_partkey Limit 5)
UNION ALL (SELECT s_suppkey, s_name FROM supplier, partsupp where s_suppkey = ps_suppkey and
ps_availqty > 200 Order By s_suppkey Limit 7);""", True, True, False, False),
                     TestQuery("U2", """(SELECT s_suppkey, s_name FROM supplier, nation where s_nationkey = n_nationkey and  n_name = 'GERMANY' order by s_suppkey desc, s_name limit 12)UNION ALL (SELECT c_custkey, c_name FROM customer,  orders where c_custkey = o_custkey and o_orderpriority = '1-URGENT' order by c_custkey, c_name desc limit 10);
""", False, True, False, False),
                     TestQuery("U3", """(SELECT c_custkey as key, c_name as name FROM customer, nation where c_nationkey = n_nationkey and  n_name = 'UNITED STATES' Order by key Limit 10)
 UNION ALL (SELECT p_partkey as key, p_name as name FROM part , lineitem where p_partkey = l_partkey and l_quantity > 35 Order By key Limit 10) 
 UNION ALL (select n_nationkey as key, r_name as name from nation, region where n_name LIKE 'B%' Order By key Limit 5)
""", False, True, False, False),
                     TestQuery("U4", """(SELECT c_custkey, c_name FROM customer,  nation where c_nationkey = n_nationkey and n_name = 'UNITED STATES' Order By c_custkey desc Limit 5) 
 UNION ALL (SELECT s_suppkey, s_name FROM supplier ,  nation where s_nationkey = n_nationkey and n_name = 'CANADA' Order By s_suppkey Limit 6) 
 UNION ALL (SELECT p_partkey, p_name FROM part ,  lineitem where p_partkey = l_partkey and l_quantity > 20 Order By p_partkey desc Limit 7) 
 UNION ALL (SELECT ps_partkey, p_name FROM part ,  partsupp where p_partkey = ps_partkey and ps_supplycost >= 1000 Order By ps_partkey Limit 8)
""", False, True, False, False),
                     TestQuery("U5", """(SELECT o_orderkey, o_orderdate, n_name FROM orders, customer, nation where o_custkey = c_custkey and c_nationkey = n_nationkey and c_name like '%0001248%'  AND o_orderdate >= '1997-01-01' order by o_orderkey Limit 20) 
 UNION ALL (SELECT l_orderkey, l_shipdate, o_orderstatus FROM lineitem, orders where l_orderkey = o_orderkey and o_orderdate < '1994-01-01'   AND l_quantity > 20   AND l_extendedprice > 1000 order by l_orderkey Limit 5);
""", False, True, False, False),
                     TestQuery("U6", """(SELECT o_clerk as name, SUM(l_extendedprice) AS total_price FROM orders, lineitem where o_orderkey = l_orderkey and o_orderdate <= '1995-01-01' GROUP BY o_clerk ORDER BY total_price DESC LIMIT 10) 
 UNION ALL (SELECT n_name as name, SUM(s_acctbal) AS total_price FROM nation ,supplier where n_nationkey = s_nationkey and n_name like '%UNITED%' GROUP BY n_name ORDER BY n_name DESC Limit 10);
""", False, True, False, False),
                     TestQuery("U7", """(SELECT     l_orderkey as key,     l_extendedprice as price,     l_partkey as s_key FROM     lineitem WHERE     l_shipdate >= DATE '1994-01-01'     AND l_shipdate < DATE '1995-01-01'     AND l_quantity > 30  Order By key Limit 20)
 UNION ALL  (SELECT     ps_partkey as key,     p_retailprice as price,     ps_suppkey as s_key FROM     partsupp,supplier,part where ps_suppkey = s_suppkey and ps_partkey = p_partkey     AND ps_supplycost < 100 Order By price Limit 20);
""", False, True, False, False),
                     TestQuery("U8", """(SELECT     c_custkey as order_id,     COUNT(*) AS total FROM
customer, orders where c_custkey = o_custkey and     o_orderdate >= '1995-01-01'
GROUP BY     c_custkey ORDER BY     total ASC LIMIT 10) UNION ALL
(SELECT     wl_orderkey as order_id,     AVG(wl_quantity) AS total FROM     orders, web_lineitem where
wl_orderkey = o_orderkey     AND o_orderdate < DATE '1996-07-01' GROUP BY     wl_orderkey ORDER BY
total DESC LIMIT 10);
""", False, True, False, False),
                     TestQuery("U9", """(select c_name, n_name from customer, nation where
c_mktsegment='BUILDING' and c_acctbal > 100 and c_nationkey
= n_nationkey) UNION ALL (select s_name, n_name from supplier,
nation where s_acctbal > 4000 and s_nationkey = n_nationkey);""", False, True, False, False),
                     TestQuery("O1", """select c_name, n_name, count(*) as total from nation RIGHT OUTER
JOIN customer ON c_nationkey = n_nationkey and c_acctbal < 1000
        GROUP BY c_name,
n_name Order by c_name, n_name desc Limit 10;""", True, False, True, False),
                     TestQuery("O2", """SELECT l_shipmode, o_shippriority ,count(*) as low_line_count FROM
lineitem LEFT OUTER JOIN orders ON ( l_orderkey = o_orderkey AND
o_totalprice > 50000 ) WHERE l_linenumber = 4 AND l_quantity < 30
GROUP BY l_shipmode, o_shippriority Order By l_shipmode Limit 5;""", True, False, True, False),
                     TestQuery("O3", """SELECT o_custkey as key, sum(c_acctbal), o_clerk, c_name from orders FULL OUTER JOIN customer on c_custkey = o_custkey and
o_orderstatus = 'F' group by o_custkey, o_clerk, c_name order by key
limit 35;""", True, False, True, False),
                     TestQuery("O4", """SELECT p_size, s_phone, ps_supplycost, n_name FROM part RIGHT
OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size >
7 LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND
s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey =
n_nationkey and n_regionkey > 3 Order by ps_supplycost asc Limit 50;""", True, False, True, False),
                     TestQuery("O5", """Select ps_suppkey, p_name, p_type from part RIGHT outer join partsupp on p_partkey=ps_partkey and p_size > 4 and ps_availqty > 3350 Order By ps_suppkey Limit 100;
""", True, False, True, False),
                     TestQuery("O6", """SELECT p_name, s_phone, ps_supplycost, n_name FROM part RIGHT OUTER JOIN partsupp ON p_partkey = ps_partkey AND p_size > 7
	LEFT OUTER JOIN supplier ON ps_suppkey = s_suppkey AND s_acctbal < 2000 FULL OUTER JOIN nation on s_nationkey = n_nationkey
	and n_regionkey > 3 Order By p_name, s_phone, ps_supplycost, n_name desc Limit 20;
""", True, False, True, False),
                     TestQuery("A1", """Select l_shipmode, count(*) as count From orders, lineitem Where
o_orderkey = l_orderkey and l_commitdate < l_receiptdate and
l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and
l_receiptdate < '1995-01-01' and l_extendedprice <= o_totalprice
and l_extendedprice <= 70000 and o_totalprice > 60000 Group By
l_shipmode Order By l_shipmode;""", True, False, False, False),
                     TestQuery("A2", """Select o_orderpriority, count(*) as order_count From orders, lineitem
Where l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and
o_orderdate < '1993-10-01' and l_commitdate <= l_receiptdate Group
By o_orderpriority Order By o_orderpriority;""", True, False, False, False),
                     TestQuery("A3", """Select l_orderkey, l_linenumber From orders, lineitem, partsupp Where
o_orderkey = l_orderkey and ps_partkey = l_partkey and ps_suppkey
= l_suppkey and ps_availqty = l_linenumber and l_shipdate >=
o_orderdate and o_orderdate >= '1990-01-01' and l_commitdate <=
l_receiptdate and l_shipdate <= l_commitdate and l_receiptdate > '1994-01-01' Order By l_orderkey Limit 7;
""", True, False, False, False),
                     TestQuery("A4", """Select s_name, count(*) as numwait From supplier, lineitem, orders,
nation Where s_suppkey = l_suppkey and o_orderkey = l_orderkey and
o_orderstatus = 'F' and l_receiptdate >= l_commitdate and s_nationkey
= n_nationkey Group By s_name Order By numwait desc Limit 100;""", True, False, False, False),
                     TestQuery("A5", """Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price, sum(l_extendedprice
* (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 -
l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*)
as count_order From lineitem Where l_shipdate <= l_receiptdate and
l_receiptdate <= l_commitdate Group By l_returnflag, l_linestatus
Order by l_returnflag, l_linestatus;""", True, False, False, False),
                     TestQuery("N1", """Select p_brand, p_type, p_size, Count(*) as supplier_cnt
 From part, partsupp
 Where p_partkey = ps_partkey
 and p_size >= 4 and p_type NOT LIKE 'SMALL PLATED%'  and p_brand <> 'Brand#45'
 Group By p_brand, p_size, p_type
 Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;""", False, False, False, True),
                     TestQuery("F1", """(SELECT c_name as name, c_acctbal as account_balance FROM orders,
customer, nation WHERE c_custkey = o_custkey and c_nationkey
= n_nationkey and c_mktsegment = 'FURNITURE' and n_name =
'INDIA' and o_orderdate between '1998-01-01' and '1998-12-05' and
o_totalprice <= c_acctbal) UNION ALL (SELECT s_name as name,
s_acctbal as account_balance FROM supplier, lineitem, orders, nation
WHERE l_suppkey = s_suppkey and l_orderkey = o_orderkey
and s_nationkey = n_nationkey and n_name = 'ARGENTINA' and
o_orderdate between '1998-01-01' and '1998-01-05' and o_totalprice >
s_acctbal and o_totalprice >= 30000 and 50000 >= s_acctbal Order by account_balance desc limit 20);
""", False, True, True, False),
                     TestQuery("F2", """(Select p_brand, o_clerk, l_shipmode From orders, lineitem, part Where
l_partkey = p_partkey and o_orderkey = l_orderkey and l_shipdate >=
o_orderdate and o_orderdate > '1994-01-01' and l_shipdate > '1995-01-01' and p_retailprice >= l_extendedprice and p_partkey < 10000 and
l_suppkey < 10000 and p_container = 'LG CAN' Order By o_clerk LIMIT
5) UNION ALL (Select p_brand, s_name, l_shipmode From lineitem,
part, supplier Where l_partkey = p_partkey and s_suppkey = s_suppkey
and l_shipdate > '1995-01-01' and s_acctbal >= l_extendedprice and
p_partkey < 15000 and l_suppkey < 14000 and p_container = 'LG CAN'
Order By s_name LIMIT 10);""", False, True, True, False),
                     TestQuery("F3", """(
	select l_orderkey, l_extendedprice as price, p_partkey from lineitem, part
	where l_partkey = p_partkey  and p_container LIKE 'JUMBO%' and p_partkey > 3000 and l_partkey < 3010
	Order by l_orderkey, price desc Limit 100
) union all (select o_orderkey, c_acctbal as price, c_custkey
from customer LEFT OUTER JOIN orders on c_custkey = o_custkey
 where c_custkey > 1000 and c_custkey < 1010 Order By price desc, o_orderkey, c_custkey Limit 100);
""", False, True, True, False),
                     TestQuery("F4", """select n_name, c_acctbal from nation LEFT OUTER JOIN customer 
                     ON n_nationkey = c_nationkey and c_nationkey > 3 and n_nationkey < 20 and 
                     c_nationkey != 10 LIMIT 200;
""", False, True, True, True),
                     TestQuery("MQ1", """SELECT l_returnflag, l_linestatus, 
       SUM(l_quantity) AS sum_qty, 
       SUM(l_extendedprice) AS sum_base_price, 
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, 
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, 
       AVG(l_quantity) AS avg_qty, 
       AVG(l_extendedprice) AS avg_price, 
       AVG(l_discount) AS avg_disc, 
       COUNT(*) AS count_order
FROM lineitem 
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '71 days' 
GROUP BY l_returnflag, l_linestatus 
ORDER BY l_returnflag, l_linestatus;""", True, False, False, False),
                     TestQuery("MQ2", """SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey 
  AND s_suppkey = ps_suppkey 
  AND p_size = 38 
  AND p_type LIKE '%TIN' 
  AND s_nationkey = n_nationkey 
  AND n_regionkey = r_regionkey 
  AND r_name = 'MIDDLE EAST'
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;""", True, False, False, False),
                     TestQuery("MQ3", """SELECT l_orderkey, 
       SUM(l_extendedprice * (1 - l_discount)) AS revenue, 
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' 
  AND c_custkey = o_custkey 
  AND l_orderkey = o_orderkey 
  AND o_orderdate < DATE '1995-03-15' 
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;""", True, False, False, False),
                     TestQuery("MQ4", """SELECT o_orderdate, o_orderpriority, COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= DATE '1997-07-01' 
  AND o_orderdate < DATE '1997-07-01' + INTERVAL '3 months'
GROUP BY o_orderdate, o_orderpriority
ORDER BY o_orderpriority
LIMIT 10;""", True, False, False, False),
                     TestQuery("MQ5", """SELECT n_name, 
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey 
  AND l_orderkey = o_orderkey 
  AND l_suppkey = s_suppkey 
  AND c_nationkey = s_nationkey 
  AND s_nationkey = n_nationkey 
  AND n_regionkey = r_regionkey 
  AND r_name = 'MIDDLE EAST' 
  AND o_orderdate >= DATE '1994-01-01' 
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '1 year'
GROUP BY n_name
ORDER BY revenue DESC
LIMIT 100;""", True, False, False, False),
                     TestQuery("MQ6", """SELECT l_shipmode, 
       SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01' 
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1 year' 
  AND l_quantity < 24
GROUP BY l_shipmode
LIMIT 100;""", True, False, False, False),
                     TestQuery("MQ10", """SELECT c_name, 
       SUM(l_extendedprice * (1 - l_discount)) AS revenue, 
       c_acctbal, n_name, c_address, c_phone, c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey 
  AND l_orderkey = o_orderkey 
  AND o_orderdate >= DATE '1994-01-01' 
  AND o_orderdate < DATE '1994-01-01' + INTERVAL '3 months' 
  AND l_returnflag = 'R' 
  AND c_nationkey = n_nationkey
GROUP BY c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;""", True, False, False, False),
                     TestQuery("MQ11", """SELECT ps_comment, 
       SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp, supplier, nation
WHERE ps_suppkey = s_suppkey 
  AND s_nationkey = n_nationkey 
  AND n_name = 'ARGENTINA'
GROUP BY ps_comment
ORDER BY value DESC
LIMIT 100;""", True, False, False, False),
                     TestQuery("MQ17", """SELECT AVG(l_extendedprice) AS avg_total
FROM lineitem, part
WHERE p_partkey = l_partkey 
  AND p_brand = 'Brand#52' 
  AND p_container = 'LG CAN';""", True, False, False, False),
                     TestQuery("MQ18", """SELECT c_name, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE c_phone LIKE '27-%' 
  AND c_custkey = o_custkey 
  AND o_orderkey = l_orderkey
GROUP BY c_name, o_orderdate, o_totalprice
ORDER BY o_orderdate, o_totalprice DESC
LIMIT 100;""", True, False, False, False),
                     TestQuery("MQ21", """SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey 
  AND o_orderkey = l1.l_orderkey 
  AND o_orderstatus = 'F' 
  AND s_nationkey = n_nationkey 
  AND n_name = 'GERMANY'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100;""", True, False, False, False),
                     TestQuery("Alaap", """select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 3000 
                        and l_quantity <= 123  group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;""", False, False, False, False),
                     TestQuery("TPCH_Q9", """select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like 'co%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;""", False, False, False, False),
                     TestQuery("TPCH_Q11", """SELECT
    ps_partkey, n_name,
    SUM(ps_supplycost * ps_availqty) AS total_value
FROM
    partsupp, supplier, nation 
where
    ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDIA'
GROUP BY
    ps_partkey, n_name
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
        FROM partsupp, supplier, nation WHERE 
        ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'INDIA'
    )
ORDER BY
    total_value DESC;
""", False, False, False, False),
                     TestQuery("TPCH_Q12", """select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode = 'SHIP'
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1995-01-01'
	and l_receiptdate < date '1995-01-01' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode;""", False, False, False, False),
                     TestQuery("TPCH_Q13", """select
	c_count, c_orderdate,
	count(*) as custdist
from
	(
		select
			c_custkey, o_orderdate,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%among%regular%'
		group by
			c_custkey, o_orderdate
	) as c_orders (c_custkey, c_count, c_orderdate)
group by
	c_count, c_orderdate
order by
	custdist desc,
	c_count desc;""", False, False, True, True),
                     TestQuery("TPCH_Q14", """select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1995-01-01' + interval '1' month;""", False, False, False, False),
                     TestQuery("ETPCH_Q15", """with revenue(supplier_no, total_revenue) as        
(select
                l_suppkey,
                sum(l_extendedprice * (1 - l_discount))
        from
                (select 
		 sl_extendedprice as l_extendedprice,
		 sl_discount as l_discount,
		 sl_partkey as l_partkey,
		 sl_suppkey as l_suppkey,
		 sl_shipdate as l_shipdate
		 from store_lineitem
		 UNION ALL
		 select
		 wl_extendedprice as l_extendedprice,
		 wl_discount as l_discount,
		 wl_partkey as l_partkey,
		 wl_suppkey as l_suppkey,
		 wl_shipdate as l_shipdate
		 from web_lineitem
        ) as lineitem
where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1995-01-01' + interval '1' month
        group by
                l_suppkey)
select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
from
        supplier,
        revenue
where
        s_suppkey = supplier_no
        and total_revenue = (
                select
                        max(total_revenue)
                from
                        revenue
        )
order by
        s_suppkey;""", False, True, False, False),
                     TestQuery("TPCH_Q16", """select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#23'
    AND p_type NOT LIKE 'MEDIUM POLISHED%' 
	and p_size IN (1, 4, 7)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;""", False, False, False, True, True),
                     TestQuery("TPCH_Q17", """SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem l
JOIN part p ON p.p_partkey = l.l_partkey
JOIN (
    SELECT l_partkey, 0.7 * AVG(l_quantity) AS threshold_quantity
    FROM lineitem
    GROUP BY l_partkey
) AS avg_lineitem ON avg_lineitem.l_partkey = l.l_partkey
WHERE p.p_brand = 'Brand#53'
  AND p.p_container = 'MED BAG'
  AND l.l_quantity < avg_lineitem.threshold_quantity;
""", False, False, False, False),
                     TestQuery("Nested_Test", """select
        s_name,
        s_address
from
        supplier,
        nation,
		partsupp,
		part
where
        s_suppkey = ps_suppkey
        and ps_partkey = p_partkey
        and p_name like '%ivory%'
		and s_nationkey = n_nationkey
        and n_name = 'FRANCE'
        and ps_availqty > (select min(c_acctbal) from customer)
order by
        s_name;""", False, False, False, False),
                     TestQuery("paper_sample", """(Select c_name as entity_name, n_name as country, o_totalprice as price
From orders JOIN customer ON c_custkey = o_custkey
and o_totalprice <= c_acctbal and c_acctbal >= 9000
JOIN nation ON c_nationkey = n_nationkey Where o_totalprice <= 15000
and c_mktsegment IN ('HOUSEHOLD','MACHINERY')
Group By c_name, n_name, o_totalprice
Order By price asc, country asc, entity_name)
UNION ALL
(Select s_name as entity_name, n_name as country,
Avg(wl_extendedprice*(1 - wl_discount)) as price
From web_lineitem, nation, orders, region, supplier
Where wl_suppkey = s_suppkey and n_nationkey = s_nationkey and
wl_orderkey = o_orderkey and n_regionkey = r_regionkey and s_acctbal
<= o_totalprice and o_totalprice <= 15000
Group By n_name, s_name
Order By price desc, country desc, entity_name)""", False, False, False, False),
                     TestQuery("ETPCH_Q1", """SELECT
    returnflag,
    linestatus,
    SUM(quantity) AS sum_qty,
    SUM(extendedprice) AS sum_base_price,
    SUM(extendedprice * (1 - discount)) AS sum_disc_price,
    SUM(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
    AVG(quantity) AS avg_qty,
    AVG(extendedprice) AS avg_price,
    AVG(discount) AS avg_disc,
    COUNT(*) AS count_order
FROM (
    SELECT
        wl_returnflag AS returnflag,
        wl_linestatus AS linestatus,
        wl_quantity AS quantity,
        wl_extendedprice AS extendedprice,
        wl_discount AS discount,
        wl_tax AS tax
    FROM web_lineitem
    WHERE wl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
    UNION ALL
    SELECT
        sl_returnflag AS returnflag,
        sl_linestatus AS linestatus,
        sl_quantity AS quantity,
        sl_extendedprice AS extendedprice,
        sl_discount AS discount,
        sl_tax AS tax
    FROM store_lineitem
    WHERE sl_shipdate <= DATE '1998-12-01' - INTERVAL '3' DAY
) AS combined
GROUP BY
    returnflag,
    linestatus
ORDER BY
    returnflag,
    linestatus;""", False, True, False, False),
                     TestQuery("ETPCH_Q3", """SELECT
    orderkey,
    SUM(extendedprice * (1 - discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM (
    SELECT
        wl_orderkey AS orderkey,
        wl_extendedprice AS extendedprice,
        wl_discount AS discount,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        web_lineitem
    WHERE
        c_mktsegment = 'FURNITURE'
        AND c_custkey = o_custkey
        AND wl_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-01-01'
        AND wl_shipdate > DATE '1995-01-01'
    UNION ALL
    SELECT
        sl_orderkey AS orderkey,
        sl_extendedprice AS extendedprice,
        sl_discount AS discount,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        store_lineitem
    WHERE
        c_mktsegment = 'FURNITURE'
        AND c_custkey = o_custkey
        AND sl_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-01-01'
        AND sl_shipdate > DATE '1995-01-01'
) AS combined_orders
GROUP BY
    orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC
LIMIT 10;""", False, True, False, False),
                     TestQuery("ETPCH_Q4", """SELECT o_orderpriority, SUM(order_count) AS order_count
FROM (
    SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
    FROM orders, web_lineitem
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
    UNION ALL
    SELECT o_orderpriority, COUNT(DISTINCT orders.o_orderkey) AS order_count
    FROM orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    GROUP BY o_orderpriority
) AS combined
GROUP BY o_orderpriority
ORDER BY o_orderpriority ASC;""", False, True, False, False),
                     TestQuery("ETPCH_Q5", """SELECT n_name, SUM(revenue) AS revenue
FROM (
    SELECT n_name, wl_extendedprice * (1 - wl_discount) AS revenue
    FROM customer, nation, orders, region, supplier, web_lineitem
    WHERE customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = nation.n_nationkey
    AND nation.n_nationkey = supplier.s_nationkey
    AND orders.o_orderkey = web_lineitem.wl_orderkey
    AND nation.n_regionkey = region.r_regionkey
    AND supplier.s_suppkey = web_lineitem.wl_suppkey
    AND region.r_name = 'ASIA'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
    UNION ALL
    SELECT n_name, sl_extendedprice * (1 - sl_discount) AS revenue
    FROM customer, nation, orders, region, store_lineitem, supplier
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND store_lineitem.sl_suppkey = supplier.s_suppkey
    AND customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = nation.n_nationkey
    AND nation.n_nationkey = supplier.s_nationkey
    AND nation.n_regionkey = region.r_regionkey
    AND region.r_name = 'ASIA'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'
) AS combined
GROUP BY n_name
ORDER BY revenue DESC, n_name ASC;""", False, True, False, False),
                     TestQuery("ETPCH_Q6", """SELECT SUM(revenue) AS revenue
FROM (
    SELECT wl_extendedprice * wl_discount AS revenue
    FROM web_lineitem
    WHERE wl_shipdate >= DATE '1993-01-01'
    AND wl_shipdate < DATE '1995-01-01'
    AND wl_discount BETWEEN 0.05 AND 0.07
    AND wl_quantity < 24
    UNION ALL
    SELECT sl_extendedprice * sl_discount AS revenue
    FROM store_lineitem
    WHERE sl_shipdate >= DATE '1993-01-01'
    AND sl_shipdate < DATE '1995-01-01'
    AND sl_discount BETWEEN 0.05 AND 0.07
    AND sl_quantity < 24
) AS combined_revenue;""", False, True, False, False),
                     TestQuery("ETPCH_Q6_1", """SELECT SUM(revenue) AS revenue
    FROM (
        SELECT wl_extendedprice * wl_discount AS revenue
        FROM web_lineitem
        WHERE wl_shipdate >= DATE '1993-01-01'
        AND wl_shipdate < DATE '1995-01-01'
        --AND wl_discount BETWEEN 0.05 AND 0.07
        AND wl_quantity < 24
        ) AS combined_revenue;""", False, True, False, False),
                     TestQuery("ETPCH_Q6_2", """SELECT *
    FROM (
        SELECT sum(wl_extendedprice ) AS revenue
        FROM web_lineitem
        WHERE wl_shipdate >= DATE '1993-01-01'
        AND wl_shipdate < DATE '1995-01-01'
        AND wl_discount BETWEEN 0.05 AND 0.07
        AND wl_quantity < 24
        UNION ALL
        SELECT sum(sl_extendedprice ) AS revenue
        FROM store_lineitem
        WHERE sl_shipdate >= DATE '1993-01-01'
        AND sl_shipdate < DATE '1995-01-01'
        AND sl_discount BETWEEN 0.05 AND 0.07
        AND sl_quantity < 24
    ) AS combined_revenue;""", False, True, False, False),
                     TestQuery("ETPCH_Q7", """SELECT supp_nation, cust_nation, l_year, SUM(revenue) as revenue
FROM (
    SELECT n1.n_name as supp_nation, n2.n1_name as cust_nation, EXTRACT(YEAR FROM wl_shipdate) as l_year, wl_extendedprice*(1 - wl_discount) as revenue 
    FROM customer, nation n1, nation1 n2, orders, supplier, web_lineitem 
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
    AND supplier.s_suppkey = web_lineitem.wl_suppkey
    AND customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = n2.n1_nationkey
    AND n1.n_nationkey = supplier.s_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n1_name = 'GERMANY') OR (n2.n1_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
    AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
    UNION ALL  
    SELECT n1.n_name as supp_nation, n2.n1_name as cust_nation, EXTRACT(YEAR FROM sl_shipdate) as l_year, sl_extendedprice*(1 - sl_discount) as revenue 
    FROM customer, nation n1, nation1 n2, orders, supplier, store_lineitem 
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND supplier.s_suppkey = store_lineitem.sl_suppkey
    AND customer.c_custkey = orders.o_custkey
    AND customer.c_nationkey = n2.n1_nationkey
    AND n1.n_nationkey = supplier.s_nationkey
    AND ((n1.n_name = 'FRANCE' AND n2.n1_name = 'GERMANY') OR (n2.n1_name = 'FRANCE' AND n1.n_name = 'GERMANY'))
    AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
) AS combined
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;""", False, True, False, False, True),
                     TestQuery("ETPCH_Q9", """SELECT 
    nation, 
    EXTRACT(YEAR FROM o_orderdate) AS o_year, 
    SUM(profit) AS sum_profit
FROM (
    SELECT 
        n_name AS nation, 
        o_orderdate, 
        (-ps_supplycost * wl_quantity + wl_extendedprice * (1 - wl_discount)) AS profit
    FROM 
        nation, orders, part, partsupp, supplier, web_lineitem
    WHERE 
        orders.o_orderkey = web_lineitem.wl_orderkey
        AND part.p_partkey = partsupp.ps_partkey
        AND partsupp.ps_partkey = web_lineitem.wl_partkey
        AND partsupp.ps_suppkey = supplier.s_suppkey
        AND supplier.s_suppkey = web_lineitem.wl_suppkey
        AND nation.n_nationkey = supplier.s_nationkey
        AND part.p_name LIKE '%co%'
    UNION ALL
    SELECT 
        n_name AS nation, 
        o_orderdate, 
        (-ps_supplycost * sl_quantity + sl_extendedprice * (1 - sl_discount)) AS profit
    FROM 
        nation, orders, part, partsupp, store_lineitem, supplier
    WHERE 
        orders.o_orderkey = store_lineitem.sl_orderkey
        AND part.p_partkey = partsupp.ps_partkey
        AND partsupp.ps_partkey = store_lineitem.sl_partkey
        AND partsupp.ps_suppkey = store_lineitem.sl_suppkey
        AND store_lineitem.sl_suppkey = supplier.s_suppkey
        AND nation.n_nationkey = supplier.s_nationkey
        AND part.p_name LIKE '%co%'
) AS combined
GROUP BY 
    nation, o_year
ORDER BY 
    nation ASC, o_year DESC;""", False, True, False, False),
                     TestQuery("ETPCH_Q10", """SELECT c_custkey, c_name, SUM(revenue) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
FROM (
    SELECT c_custkey, c_name, sl_extendedprice * (1 - sl_discount) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, nation, orders, store_lineitem
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
    AND customer.c_nationkey = nation.n_nationkey
    AND customer.c_custkey = orders.o_custkey
    AND store_lineitem.sl_returnflag = 'R'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
    UNION ALL
    SELECT c_custkey, c_name, wl_extendedprice * (1 - wl_discount) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment
    FROM customer, nation, orders, web_lineitem
    WHERE customer.c_nationkey = nation.n_nationkey
    AND orders.o_orderkey = web_lineitem.wl_orderkey
    AND customer.c_custkey = orders.o_custkey
    AND web_lineitem.wl_returnflag = 'R'
    AND orders.o_orderdate BETWEEN '1995-01-01' AND '1995-03-31'
) AS combined
GROUP BY c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment
ORDER BY revenue DESC, c_custkey ASC, c_name ASC, c_acctbal ASC, c_phone ASC, n_name ASC, c_address ASC, c_comment ASC
LIMIT 20;""", False, True, False, False),
                     TestQuery("ETPCH_Q12", """SELECT shipmode, 
       SUM(high_line_count) AS high_line_count, 
       SUM(low_line_count) AS low_line_count
FROM (
    SELECT sl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
    FROM orders, store_lineitem 
    WHERE orders.o_orderkey = store_lineitem.sl_orderkey
      AND store_lineitem.sl_shipdate < store_lineitem.sl_commitdate
      AND store_lineitem.sl_commitdate < store_lineitem.sl_receiptdate
      AND store_lineitem.sl_shipmode IN ('SHIP', 'TRUCK')
      AND store_lineitem.sl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY sl_shipmode
    UNION ALL  
    SELECT wl_shipmode AS shipmode, 
           SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count, 
           SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count 
    FROM orders, web_lineitem 
    WHERE orders.o_orderkey = web_lineitem.wl_orderkey
      AND web_lineitem.wl_shipdate < web_lineitem.wl_commitdate
      AND web_lineitem.wl_commitdate < web_lineitem.wl_receiptdate
      AND web_lineitem.wl_shipmode IN ('SHIP', 'TRUCK')
      AND web_lineitem.wl_receiptdate BETWEEN '1995-01-01' AND '1995-12-31'
    GROUP BY wl_shipmode
) AS combined
GROUP BY shipmode;""", False, True, False, False),
                     TestQuery("ETPCH_Q14", """SELECT SUM(promo_revenue) / SUM(total_revenue) * 100 AS promo_revenue_percentage
FROM (
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount) ELSE 0 END) AS promo_revenue,
        SUM(store_lineitem.sl_extendedprice * (1 - store_lineitem.sl_discount)) AS total_revenue
    FROM 
        part, store_lineitem
    WHERE 
        part.p_partkey = store_lineitem.sl_partkey
        AND store_lineitem.sl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
    UNION ALL
    SELECT 
        SUM(CASE WHEN part.p_type LIKE 'PROMO%' THEN web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount) ELSE 0 END) AS promo_revenue,
        SUM(web_lineitem.wl_extendedprice * (1 - web_lineitem.wl_discount)) AS total_revenue
    FROM 
        part, web_lineitem
    WHERE 
        part.p_partkey = web_lineitem.wl_partkey
        AND web_lineitem.wl_shipdate BETWEEN '1995-01-01' AND '1995-01-31'
) AS combined_revenue;""", False, True, False, False),

                     TestQuery("ETPCH_Q13", """select
        c_count, c_orderdate,
        count(*) as custdist
from
        (
                select
                        c_custkey, o_orderdate,
                        count(o_orderkey)
                from
                        customer left outer join orders on
                                c_custkey = o_custkey
                                and o_comment not like '%special%requests%'
                group by
                        c_custkey, o_orderdate
        ) as c_orders (c_custkey, c_count, c_orderdate)
group by
        c_count, c_orderdate
order by
        custdist desc,
        c_count desc;""", False, True, True, False),
                     TestQuery("ETPCH_Q2", """
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey limit 100;""", False, True, False, False),
                     TestQuery("ETPCH_Q22", """select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
from
        (
                select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                from
                        customer
                where
                        substring(c_phone from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c1_acctbal)
                                from
                                        customer1
                                where
                                        c1_acctbal > 0.00
                                        and substring(c1_phone from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
group by
        cntrycode
order by
        cntrycode;""", False, True, False, False, True),
                     TestQuery("ETPCH_Q8", """select
        o_year,
        sum(case
                when nation = 'INDIA' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        wl_extendedprice * (1 - wl_discount) as volume,
                        n2.n1_name as nation
                from
                        part,
                        supplier,
                        web_lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation1 n2,
                        region
                where
                        p_partkey = wl_partkey
                        and s_suppkey = wl_suppkey
                        and wl_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'ASIA'
                        and s_nationkey = n2.n1_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;""", False, True, False, False),
                     TestQuery("ETPCH_Q11", """SELECT
    ps_partkey, n_name,
    SUM(ps_supplycost * ps_availqty) AS total_value
FROM
    partsupp, supplier, nation 
where
    ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'INDIA'
GROUP BY
    ps_partkey, n_name
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT SUM(ps_supplycost * ps_availqty) * 0.00001
        FROM partsupp, supplier, nation WHERE 
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'INDIA'
    )
ORDER BY
    total_value DESC;""", False, True, False, False),
                     TestQuery("ETPCH_Q16", """select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
from
        partsupp,
        part
where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#23'
    AND p_type NOT LIKE 'MEDIUM POLISHED%' 
        and p_size IN (1, 4, 7)
        and ps_suppkey not in (
                select
                        s_suppkey
                from
                        supplier
                where
                        s_comment like '%Customer%Complaints%'
        )
group by
        p_brand,
        p_type,
        p_size
order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size;""", False, True, False, False, True),
                     TestQuery("ETPCH_Q17", """select sum(wl_extendedprice) / 7.0 as avg_yearly
from
        web_lineitem,
        part
where
        p_partkey = wl_partkey
        and p_brand = 'Brand#53'
        and p_container = 'MED BAG'
        and wl_quantity < (
                select
                        0.7 * avg(wl_quantity)
                from
                        web_lineitem
                where
                        wl_partkey = p_partkey
        );""", False, True, False, False),
                     TestQuery("ETPCH_Q19", """select
        sum(wl_extendedprice* (1 - wl_discount)) as revenue
from
        web_lineitem,
        part
where
        (
                p_partkey = wl_partkey
                and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and wl_quantity >= 1 and wl_quantity <= 1 + 10
                and p_size between 1 and 5
                and wl_shipmode in ('AIR', 'AIR REG')
                and wl_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = wl_partkey
                and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and wl_quantity >= 10 and wl_quantity <= 10 + 10
                and p_size between 1 and 10
                and wl_shipmode in ('AIR', 'AIR REG')
                and wl_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = wl_partkey
                and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and wl_quantity >= 20 and wl_quantity <= 20 + 10
                and p_size between 1 and 15
                and wl_shipmode in ('AIR', 'AIR REG')
                and wl_shipinstruct = 'DELIVER IN PERSON'
        );""", False, True, False, False, True),
                     TestQuery("ETPCH_Q20", """select
        s_name,
        s_address
from
        supplier,
        nation
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        part
                                where
                                        p_name like '%ivory%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(wl_quantity)
                                from
                                        web_lineitem
                                where
                                        wl_partkey = ps_partkey
                                        and wl_suppkey = ps_suppkey
                                        and wl_shipdate >= date '1995-01-01'
                                        and wl_shipdate < date '1995-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'FRANCE'
order by
        s_name;""", False, True, False, False),
                     TestQuery("ETPCH_Q21", """""", False, True, False, False),
                     TestQuery("ETPCH_Q23", """SELECT   RIGHT(c_address, 5) AS city,
         p_brand             AS part_brand
FROM     customer,
         orders o1,
         order1 o2,
         store_lineitem,
         web_lineitem,
         part
WHERE    c_custkey = o1.o_custkey
AND      c_custkey = o2.o1_custkey 
AND      o1.o_orderkey = wl_orderkey
AND      wl_returnflag = 'A' 
AND      o2.o1_orderkey = sl_orderkey
AND      sl_returnflag = 'N' 
AND      wl_partkey = sl_partkey
AND      sl_partkey = p_partkey
AND      o1.o_orderdate < o2.o1_orderdate
AND      wl_receiptdate < sl_receiptdate 
AND      o1.o_orderdate BETWEEN date '1995-01-01' AND date '1995-12-31'
AND      o2.o1_orderdate BETWEEN date '1995-01-01' AND date '1995-12-31'
GROUP BY RIGHT(c_address, 5),
         p_brand 
ORDER BY city, part_brand;""", False, True, False, False),
                     TestQuery("ETPCH_Q24", """select c_address as city 
from customer, 
orders o1, 
order1 o2, 
store_lineitem, 
web_lineitem w, 
part, 
web_lineitem1 w1, 
partsupp ps1, 
partsupp1 ps2
where c_custkey = o1.o_custkey and c_custkey = o2.o1_custkey 
and o1.o_orderkey = sl_orderkey and sl_returnflag = 'A'
and o2.o1_orderkey = w.wl_orderkey and w.wl_returnflag = 'N'
and w.wl_partkey = sl_partkey and sl_partkey = p_partkey and w1.wl1_partkey = p_partkey
and sl_receiptdate < w.wl_receiptdate 
and o1.o_orderdate < o2.o1_orderdate
and w.wl_suppkey = ps1.ps_suppkey and w1.wl1_suppkey = ps2.ps1_suppkey
and ps2.ps1_availqty >= ps1.ps_availqty
and o1.o_orderdate between date '1995-01-01' and date '1995-12-31'
and o2.o1_orderdate between date '1995-01-01' and date '1995-12-31'
group by c_address;""", False, True, False, False),

                     ]
    return test_workload


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    workload = create_workload()

    workload_dict = {}
    for elem in workload:
        workload_dict[elem.qid] = workload.index(elem)

    # print(workload_dict)

    qid = sys.argv[1]
    hq = workload[workload_dict[qid]]
    query = hq.query
    conn = ConnectionHelperFactory().createConnectionHelper()
    conn.config.detect_union = hq.union
    conn.config.detect_oj = hq.oj
    conn.config.detect_nep = hq.nep
    conn.config.use_cs2 = hq.cs2
    conn.config.detect_or = hq.orf

    print(f"Flags: Scale Down {conn.config.scale_down}, Union {conn.config.detect_union}, OJ {conn.config.detect_oj}, "
          f"NEP {conn.config.detect_nep}, CS2 {conn.config.use_cs2}")

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    factory = PipeLineFactory()
    token = factory.init_job(conn, query)
    factory.doJob(query, token)
    result = factory.result

    if result is not None:
        print("============= Given Query ===============")
        print(query)
        print("=========== Extracted Query =============")
        print(result)
        print("================ Profile ================")
        pipe = factory.get_pipeline_obj(token)
        pipe.time_profile.print()
    else:
        print("I had some Trouble! Check the log file for the details..")
