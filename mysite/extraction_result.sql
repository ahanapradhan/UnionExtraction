
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
