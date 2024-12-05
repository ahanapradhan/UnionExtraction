
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_address as city 
from customer, 
orders o1, 
orders1 o2, 
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
and w.wl_suppkey = ps1.ps_suppkey and w1.wl1_suppkey = ps2.ps1_suppkey
and ps2.ps1_availqty >= ps1.ps_availqty
and o1.o_orderdate between date '1995-01-01' and date '1995-12-31'
and o2.o1_orderdate between date '1995-01-01' and date '1995-12-31'
group by c_address
;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_address as city 
from customer, 
orders o1, 
orders1 o2, 
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
and w.wl_suppkey = ps1.ps_suppkey and w1.wl1_suppkey = ps2.ps1_suppkey
and ps2.ps1_availqty >= ps1.ps_availqty
and o1.o_orderdate between date '1995-01-01' and date '1995-12-31'
and o2.o1_orderdate between date '1995-01-01' and date '1995-12-31'
group by c_address
;
 --- extracted query:
  
 Select c_address as city 
 From customer, orders, orders1, part, partsupp, partsupp1, store_lineitem, web_lineitem, web_lineitem1 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_custkey = orders1.o1_custkey
 and orders.o_orderkey = store_lineitem.sl_orderkey
 and orders1.o1_orderkey = web_lineitem.wl_orderkey
 and part.p_partkey = store_lineitem.sl_partkey
 and store_lineitem.sl_partkey = web_lineitem.wl_partkey
 and web_lineitem.wl_partkey = web_lineitem1.wl1_partkey
 and partsupp.ps_suppkey = web_lineitem.wl_suppkey
 and partsupp.ps_suppkey = web_lineitem.wl_suppkey
 and partsupp1.ps1_suppkey = web_lineitem1.wl1_suppkey
 and store_lineitem.sl_receiptdate < web_lineitem.wl_receiptdate
 and partsupp.ps_availqty <= partsupp1.ps1_availqty
 and store_lineitem.sl_returnflag = 'A'
 and web_lineitem.wl_returnflag = 'N'
 and orders.o_orderdate between '1995-01-01' and '1995-12-31'
 and orders1.o1_orderdate between '1995-01-01' and '1995-12-31'
 and web_lineitem.wl_receiptdate >= '1995-03-01' 
 Group By c_address 
 Order By city asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_address as city 
from customer, 
orders o1, 
orders1 o2, 
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
group by c_address
;
 --- extracted query:
  
 Select c_address as city 
 From customer, orders, orders1, part, partsupp, partsupp1, store_lineitem, web_lineitem, web_lineitem1 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_custkey = orders1.o1_custkey
 and orders.o_orderkey = store_lineitem.sl_orderkey
 and orders1.o1_orderkey = web_lineitem.wl_orderkey
 and part.p_partkey = store_lineitem.sl_partkey
 and store_lineitem.sl_partkey = web_lineitem.wl_partkey
 and web_lineitem.wl_partkey = web_lineitem1.wl1_partkey
 and partsupp.ps_suppkey = web_lineitem.wl_suppkey
 and partsupp.ps_suppkey = web_lineitem.wl_suppkey
 and partsupp1.ps1_suppkey = web_lineitem1.wl1_suppkey
 and orders.o_orderdate < orders1.o1_orderdate
 and orders.o_orderdate < web_lineitem.wl_receiptdate
 and store_lineitem.sl_receiptdate < web_lineitem.wl_receiptdate
 and partsupp.ps_availqty <= partsupp1.ps1_availqty
 and store_lineitem.sl_returnflag = 'A'
 and web_lineitem.wl_returnflag = 'N'
 and orders.o_orderdate >= '1995-01-01'
 and orders1.o1_orderdate <= '1995-12-31'
 and web_lineitem.wl_receiptdate >= '1995-03-01' 
 Group By c_address 
 Order By city asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 with revenue(supplier_no, total_revenue) as        
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
        ) as lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-01-01'
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
        s_suppkey;
 --- extracted query:
 connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  database "tpch_tiny" does not exist

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 with revenue(supplier_no, total_revenue) as        
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
        ) as lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-01-01'
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
        s_suppkey;
 --- extracted query:
 connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  database "tpch_tiny" does not exist

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 with revenue(supplier_no, total_revenue) as        
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
        ) as lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-01-01'
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
        s_suppkey;
 --- extracted query:
 too many values to unpack (expected 2)
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
 too many values to unpack (expected 3)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 too many values to unpack (expected 3)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 too many values to unpack (expected 3)
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
 Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { N, ', j, T, o, e, p,  , l, n, y, s, a, c, u, b, i, r, t }

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
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

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
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { supplier }, FROM(q2) = { partsupp }, FROM(q3) = { region }, FROM(q4) = { part }, FROM(q5) = { lineitem }, FROM(q6) = { orders }, FROM(q7) = { nation }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { nation }, FROM(q2) = { supplier }, FROM(q3) = { partsupp }, FROM(q4) = { region }, FROM(q5) = { orders }, FROM(q6) = { part }, FROM(q7) = { lineitem }

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
  
 (Select l_orderkey as key 
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
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { region }, FROM(q2) = { lineitem }, FROM(q3) = { supplier }, FROM(q4) = { orders }, FROM(q5) = { part }, FROM(q6) = { nation }, FROM(q7) = { partsupp }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { orders }, FROM(q2) = { supplier }, FROM(q3) = { nation }, FROM(q4) = { partsupp }, FROM(q5) = { lineitem }, FROM(q6) = { part }, FROM(q7) = { region }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = {  }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = {  }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = {  }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select 5363650 as key);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select 1002 as key);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select 1591073 as key);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

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
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
  
 (Select o_orderkey as key 
 From customer, lineitem, orders 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0)
 UNION ALL  
 (Select o_orderkey as key 
 From lineitem, orders, part 
 Where lineitem.l_orderkey = orders.o_orderkey
 and orders.o_totalprice <= 905.0);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { customer, lineitem, orders }, FROM(q2) = { orders, customer, part }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { lineitem, orders }, FROM(q2) = { customer, orders }

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 (select l_partkey as key from lineitem, part where l_partkey = p_partkey and l_extendedprice <= 905) union all (select l_orderkey as key from lineitem, orders where l_orderkey = o_orderkey and o_totalprice <= 905) union all (select o_orderkey as key from customer, orders where c_custkey = o_custkey and o_totalprice <= 890);
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!Could not extract the query due to errors.
Here's what I have as a half-baked answer:
FROM(q1) = { orders, lineitem }, FROM(q2) = { lineitem, part }

 --- END OF ONE EXTRACTION EXPERIMENT
