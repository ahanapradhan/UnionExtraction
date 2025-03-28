
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
 and lineitem.l_extendedprice <= 905.0);
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
 SELECT c_name AS entity_name, n_name AS country, o_totalprice
AS price FROM customer, orders, nation
WHERE o_comment = c_comment AND o_totalprice >= c_acctbal
AND o_totalprice < 50000 AND c_acctbal >= 1000
AND c_nationkey = n_nationkey
AND c_mktsegment IN ('HOUSEHOLD', 'MACHINERY');
 --- extracted query:
 Cannot do database minimizationSome problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name AS entity_name, n_name AS country, o_totalprice
AS price FROM customer, orders, nation
WHERE o_comment = c_comment AND o_totalprice >= c_acctbal
AND o_totalprice < 50000 AND c_acctbal >= 1000
AND c_nationkey = n_nationkey
AND c_mktsegment IN ('HOUSEHOLD', 'MACHINERY');
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name AS entity_name, n_name AS country, o_totalprice
AS price FROM customer, orders, nation
WHERE o_comment = c_comment AND o_totalprice >= c_acctbal
AND o_totalprice < 50000 AND c_acctbal >= 1000
AND c_nationkey = n_nationkey
AND c_mktsegment IN ('HOUSEHOLD', 'MACHINERY');
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name AS entity_name, n_name AS country, o_totalprice
AS price FROM customer, orders, nation
WHERE o_comment = c_comment AND o_totalprice >= c_acctbal
AND o_totalprice < 50000 AND c_acctbal >= 1000
AND c_nationkey = n_nationkey
AND c_mktsegment IN ('HOUSEHOLD', 'MACHINERY');
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, o_totalprice as price 
 From customer, nation, orders 
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_comment = orders.o_comment
 and customer.c_acctbal <= orders.o_totalprice
 and customer.c_mktsegment = 'MACHINERY'
 and customer.c_comment = 'ording to the furiously regular'
 and customer.c_acctbal >= 1000.0
 and orders.o_totalprice <= 49999.99;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name as entity_name, n_name as country, o_totalprice as
price from customer, orders, nation where
o_totalprice = c_acctbal and c_nationkey =
n_nationkey and c_mktsegment IN ('HOUSEHOLD','MACHINERY');
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, c_acctbal as price 
 From customer, nation, orders 
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_acctbal = orders.o_totalprice
 and customer.c_mktsegment = 'MACHINERY';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_name as entity_name, n_name as country, o_totalprice as
price from customer, orders, nation where
o_totalprice = c_acctbal and c_nationkey =
n_nationkey and c_mktsegment IN ('HOUSEHOLD','MACHINERY');
 --- extracted query:
  
 Select c_name as entity_name, n_name as country, c_acctbal as price 
 From customer, nation, orders 
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_acctbal = orders.o_totalprice
 and customer.c_mktsegment IN ('HOUSEHOLD', 'MACHINERY');
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
 SELECT name, phone_num
FROM ((
SELECT
c_name AS name, c_phone as phone_num
FROM customer, orders
	WHERE c_custkey = o_custkey and c_acctbal <= o_totalprice
)
UNION ALL
(-- subquery 𝑞2
SELECT s_name AS name, s_phone as phone_num
	FROM supplier
WHERE  EXISTS (select wl_suppkey from web_lineitem, orders 
			   WHERE wl_orderkey = o_orderkey 
			   AND s_acctbal <= o_totalprice 
			   AND wl_commitdate = wl_receiptdate
			  )
	)) as people
GROUP BY name, phone_num Limit 100;
 --- extracted query:
  
 (Select s_name as name, s_phone as phone_num 
 From orders, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and web_lineitem.wl_commitdate = web_lineitem.wl_receiptdate
 and supplier.s_acctbal <= orders.o_totalprice 
 Group By s_name, s_phone 
 Order By phone_num asc, name desc 
 Limit 100)
 UNION ALL  
 (Select c_name as name, c_phone as phone_num 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal <= orders.o_totalprice 
 Group By c_name, c_phone 
 Order By phone_num asc, name desc 
 Limit 100);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT name, phone_num
FROM (
(-- subquery 𝑞2
SELECT s_name AS name, s_phone as phone_num
	FROM supplier
WHERE  EXISTS (select wl_suppkey from web_lineitem, orders 
			   WHERE wl_orderkey = o_orderkey 
			   AND s_acctbal <= o_totalprice 
			   AND wl_commitdate = wl_receiptdate
			  )
	)) as people
GROUP BY name, phone_num Limit 100;
 --- extracted query:
  
 Select s_name as name, s_phone as phone_num 
 From orders, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and web_lineitem.wl_commitdate = web_lineitem.wl_receiptdate
 and supplier.s_acctbal <= orders.o_totalprice 
 Group By s_name, s_phone 
 Order By name asc, phone_num desc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT name, phone_num
FROM (
(-- subquery 𝑞2
SELECT s_name AS name, s_phone as phone_num
	FROM supplier
WHERE  EXISTS (select wl_suppkey from web_lineitem, orders 
			   WHERE wl_orderkey = o_orderkey 
			   AND s_acctbal <= o_totalprice 
			   AND wl_commitdate >= wl_receiptdate
			  )
	)) as people
GROUP BY name, phone_num Limit 100;
 --- extracted query:
  
 Select s_name as name, s_phone as phone_num 
 From orders, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and supplier.s_acctbal <= orders.o_totalprice
 and web_lineitem.wl_receiptdate <= web_lineitem.wl_commitdate 
 Group By s_name, s_phone 
 Order By name asc, phone_num asc 
 Limit 100;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT s_name AS name, s_phone as phone
FROM supplier, web_lineitem, orders WHERE wl_orderkey = o_orderkey
AND s_suppkey = wl_suppkey AND s_acctbal <= o_totalprice
AND wl_commitdate = wl_receiptdate GROUP BY s_name, s_phone;
 --- extracted query:
  
 Select s_name as name, s_phone as phone 
 From orders, supplier, web_lineitem 
 Where orders.o_orderkey = web_lineitem.wl_orderkey
 and supplier.s_suppkey = web_lineitem.wl_suppkey
 and web_lineitem.wl_commitdate = web_lineitem.wl_receiptdate
 and supplier.s_acctbal <= orders.o_totalprice 
 Group By s_name, s_phone 
 Order By name asc, phone asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select * from tag;
 --- extracted query:
 connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select * from tag;
 --- extracted query:
 connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select * from tag;
 --- extracted query:
 connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(*), ac_display_name, s_site_name from account, so_user, site where ac_id = su_account_id and su_site_id = s_site_idgroup by ac_display_name, s_site_name;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(*), ac_display_name, s_site_name from account, so_user, site where ac_id = su_account_id and su_site_id = s_site_id group by ac_display_name, s_site_name;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(*) from tag, site, question, tag_question
where
s_site_name='3dprinting' and
t_name='material' and
t_site_id = s_site_id and
q_site_id = s_site_id and
tq_site_id = s_site_id and
tq_question_id = q_id and
tq_tag_id = t_id;
 --- extracted query:
 invalid input syntax for type date: "None"
LINE 1: UPDATE unmasque.question  SET q_deletion_date='None';
                                                      ^
Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(*) from tag, site, question, tag_question
where
s_site_name='3dprinting' and
t_name='material' and
t_site_id = s_site_id and
q_site_id = s_site_id and
tq_site_id = s_site_id and
tq_question_id = q_id and
tq_tag_id = t_id;
 --- extracted query:
 invalid input syntax for type date: "None"
LINE 1: UPDATE unmasque.question  SET q_closed_date='None';
                                                    ^
Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(*) from tag, site, question, tag_question
where
s_site_name='3dprinting' and
t_name='material' and
t_site_id = s_site_id and
q_site_id = s_site_id and
tq_site_id = s_site_id and
tq_question_id = q_id and
tq_tag_id = t_id;
 --- extracted query:
  
 Select Count(*) as count 
 From question, site, tag, tag_question 
 Where question.q_id = tag_question.tq_question_id
 and question.q_site_id = site.s_site_id
 and site.s_site_id = tag.t_site_id
 and tag.t_site_id = tag_question.tq_site_id
 and tag.t_id = tag_question.tq_tag_id
 and site.s_site_name = '3dprinting'
 and tag.t_name = 'material';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_display_name) 
	from account, so_user, badge b1, badge1 b2 
	where
--account.ac_website_url != '' and
account.ac_id = so_user.su_account_id and
b1.b_site_id = so_user.su_site_id and
b1.b_user_id = so_user.su_id and
b1.b_name = 'Supporter' and
b2.b1_site_id = so_user.su_site_id and
b2.b1_user_id = so_user.su_id and
b2.b1_name = 'Student' and
b2.b1_date > b1.b_date --+ '3 months'::interval
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_display_name) 
	from account, so_user, badge b1, badge1 b2 
	where
--account.ac_website_url != '' and
account.ac_id = so_user.su_account_id and
b1.b_site_id = so_user.su_site_id and
b1.b_user_id = so_user.su_id and
b1.b_name = 'Supporter' and
b2.b1_site_id = so_user.su_site_id and
b2.b1_user_id = so_user.su_id and
b2.b1_name = 'Student' and
b2.b1_date > b1.b_date --+ '3 months'::interval
 --- extracted query:
  
 Select Count(*) as count 
 From account, badge, badge1, so_user 
 Where account.ac_id = so_user.su_account_id
 and badge.b_site_id = badge1.b1_site_id
 and badge1.b1_site_id = so_user.su_site_id
 and badge.b_user_id = badge1.b1_user_id
 and badge1.b1_user_id = so_user.su_id
 and badge.b_date < badge1.b1_date
 and badge.b_name = 'Supporter'
 and badge1.b1_name = 'Student';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_display_name) 
	from account, so_user, badge b1, badge1 b2 
	where
--account.ac_website_url != '' and
account.ac_id = so_user.su_account_id and
b1.b_site_id = so_user.su_site_id and
b1.b_user_id = so_user.su_id and
b1.b_name = 'Supporter' and
b2.b1_site_id = so_user.su_site_id and
b2.b1_user_id = so_user.su_id and
b2.b1_name = 'Student' and
b2.b1_date > b1.b_date --+ '3 months'::interval
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, site, so_user, question q, post_link pl, tag, tag_question tq where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id and
pl.pl_site_id = q.q_site_id and
pl.pl_post_id_to = q.q_id and

tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id and

q.q_creation_date > '2014-01-01'::date and

tq.tq_site_id = tag.t_site_id and
tq.tq_tag_id = tag.t_id and
tq.tq_question_id = q.q_id and

q.q_owner_user_id = so_user.su_id and
q.q_site_id = so_user.su_site_id and
so_user.su_reputation > 67 and

account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 set search_path='experiments';
select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 invalid input syntax for type date: "None"
LINE 1: UPDATE unmasque.answer  SET an_deletion_date='None';
                                                     ^
Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 invalid input syntax for type date: "None"
LINE 1: UPDATE unmasque.question  SET q_deletion_date='None';
                                                      ^
Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 invalid input syntax for type date: "None"
LINE 1: UPDATE unmasque.question  SET q_closed_date='None';
                                                    ^
Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select count(distinct account.ac_id) from
account, 
	site, 
	so_user, 
	question q, post_link pl, 
	tag 
	where
not exists (select * from answer a where a.an_site_id = q.q_site_id and a.an_question_id = q.q_id) and
site.s_site_name = 'stackoverflow' and
site.s_site_id = q.q_site_id 
	and
pl.pl_site_id = q.q_site_id 
	and
pl.pl_post_id_to = q.q_id 
	and
tag.t_name = 'perl' and
tag.t_site_id = q.q_site_id 
	and
q.q_creation_date > '2014-01-01'::date and
q.q_owner_user_id = so_user.su_id 
	and
q.q_site_id = so_user.su_site_id 
	and
so_user.su_reputation > 67 and
account.ac_id = so_user.su_account_id and
account.ac_website_url != '';
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT
