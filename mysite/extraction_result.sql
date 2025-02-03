
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4)
 UNION ALL  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008');
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 
        (select n_name, c_acctbal from nation, customer where c_nationkey = n_nationkey and n_regionkey > 3)
        UNION ALL
        (select r_name, s_acctbal from region, nation, supplier where r_regionkey = n_regionkey 
        and n_nationkey = s_nationkey and s_name = 'Supplier#000000008');
        
 --- extracted query:
  
 (Select r_name as n_name, s_acctbal as c_acctbal 
 From nation, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and supplier.s_name = 'Supplier#000000008')
 UNION ALL  
 (Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey >= 4);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        c_count desc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        c_count desc;
 --- extracted query:
  
 Select o_orderdate as c_count, Count(*) as c_orderdate, 1 as custdist 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey 
 Group By o_orderdate 
 Order By c_count desc, custdist asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        lineitem,
        part
where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#12'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 1 and l_quantity <= 1 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#23'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 10 and l_quantity <= 10 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#34'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 20 and l_quantity <= 20 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        );
 --- extracted query:
  
 Select l_extendedprice*(1 - l_discount) as revenue 
 From lineitem, part 
 Where lineitem.l_partkey = part.p_partkey
 and (lineitem.l_quantity between 1.00 and 11.00 OR lineitem.l_quantity between 10.00 and 20.00 OR lineitem.l_quantity between 20.00 and 30.00)
 and part.p_brand IN ('Brand#12', 'Brand#23', 'Brand#34')
 and part.p_container IN ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG', 'MED BAG', 'MED BOX', 'MED PACK', 'MED PKG', 'SM BOX', 'SM CASE', 'SM PACK', 'SM PKG')
 and (part.p_size between 1 and 10 OR part.p_size between 1 and 15)
 and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
 and lineitem.l_shipmode = 'AIR'
 and part.p_size between 1 and 5;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        o_year,
        sum(case
                when nation = 'INDIA' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'ASIA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly
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
 --- extracted query:
 connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT SUM(l.l_extendedprice) / 7.0 AS avg_yearly
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
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 "select
        s_name,
        count(*) as numwait
from
        supplier,
        lineitem l1,
        orders,
        nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'ARGENTINA'
group by
        s_name
order by
        numwait desc,
        s_name;
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        s_name,
        count(*) as numwait
from
        supplier,
        lineitem l1,
        orders,
        nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'ARGENTINA'
group by
        s_name
order by
        numwait desc,
        s_name;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
                                        avg(c_acctbal)
                                from
                                        customer
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone from 1 for 2) in
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
        cntrycode;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
                                        avg(ic_acctbal)
                                from
                                        inner_customer
                                where
                                        ic_acctbal > 0.00
                                        and substring(ic_phone from 1 for 2) in
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
        cntrycode;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
                                        p_name like '%cornsilk%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1995-01-01'
                                        and l_shipdate < date '1995-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
order by
        s_name;
 --- extracted query:
  
 Select s_name, s_address 
 From lineitem, nation, part, partsupp, supplier 
 Where lineitem.l_partkey = part.p_partkey
 and part.p_partkey = partsupp.ps_partkey
 and lineitem.l_suppkey = partsupp.ps_suppkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'CANADA'
 and lineitem.l_quantity <= 2589.99
 and lineitem.l_shipdate between '1995-01-01' and '1995-12-31'
 and part.p_name LIKE '%cornsilk%'
 and partsupp.ps_availqty >= 10 
 Order By s_name asc;
 select n_regionkey from nation where exists (select * from customer, orders where c_custkey = o_custkey and o_totalprice < 10000);
 --- extracted query:
  
 Select n_regionkey 
 From customer, nation, orders 
 Where customer.c_custkey = orders.o_custkey
 and orders.o_totalprice <= 9999.99;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1995-01-01'
                                        and l_shipdate < date '1995-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'FRANCE'
        nation,
		partsupp,
		part
where
        s_suppkey = ps_suppkey
        and ps_partkey = p_partkey
        and p_name like '%ivory%'
		and s_nationkey = n_nationkey
        and n_name = 'FRANCE'
        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1995-01-01'
                                        and l_shipdate < date '1995-01-01' + interval '1' year
         )
order by
        s_name;
 --- extracted query:
 Cannot do database minimizationSome problem in Regular mutation pipeline. Aborting extraction!connection already closed
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        and ps_availqty > (select max(c_acctbal) from customer)
order by
        s_name;
 --- extracted query:
 Cannot do database minimizationSome problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        and ps_availqty > (select sum(c_acctbal) from customer where c_phone LIKE '%8-1123%')
order by
        s_name;
 --- extracted query:
  
 Select s_name, s_address 
 From lineitem, nation, part, partsupp, supplier
 Where lineitem.l_partkey = part.p_partkey
 and part.p_partkey = partsupp.ps_partkey
 and lineitem.l_suppkey = partsupp.ps_suppkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_nationkey = supplier.s_nationkey
 and nation.n_name = 'FRANCE'
 and lineitem.l_quantity <= 9687.99
 and lineitem.l_shipdate between '1995-01-01' and '1995-12-31'
 and part.p_name LIKE '%ivory%'
 and partsupp.ps_availqty >= 12 
 Order By s_name asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
        p_partkey;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
        p_partkey;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
        p_partkey;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
        p_partkey;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
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
        p_partkey;
 --- extracted query:
  
 Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment 
 From nation, part, partsupp, region, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and nation.n_regionkey = region.r_regionkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and part.p_size = 15
 and region.r_name = 'EUROPE'
 and part.p_type LIKE '%BRASS' 
 Order By s_acctbal desc, n_name asc, s_name asc, p_partkey asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue desc Limit  20;
 --- extracted query:
  
 Select c_name, Sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment 
 From customer, lineitem, nation, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation.n_nationkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_returnflag = 'R'
 and orders.o_orderdate between '1994-01-01' and '1994-03-31' 
 Group By c_acctbal, c_address, c_comment, c_name, c_phone, n_name 
 Order By revenue desc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc 
 Limit 20;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n2_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation1 n1,
                        nation2 n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n2_nationkey
                        and (
                                (n1.n_name = 'GERMANY' and n2.n2_name = 'FRANCE')
                                or (n1.n_name = 'FRANCE' and n2.n2_name = 'GERMANY')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year;
 --- extracted query:
  
 Select 'FRANCE                   ' as supp_nation, 'GERMANY                  ' as cust_nation, 1995.0 as l_year, l_extendedprice*(1 - l_discount) as revenue 
 From customer, lineitem, nation1, nation2, orders, supplier 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_nationkey = nation2.n2_nationkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_suppkey = supplier.s_suppkey
 and nation1.n_nationkey = supplier.s_nationkey
 and nation1.n_name IN ('FRANCE', 'GERMANY')
 and nation2.n2_name IN ('FRANCE', 'GERMANY')
 and lineitem.l_shipdate between '1995-01-01' and '1996-12-31';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        o_year,
        sum(case
                when nation = 'INDIA' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n2_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation1 n1,
                        nation2 n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'ASIA'
                        and s_nationkey = n2.n2_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        o_year,
        sum(case
                when nation = 'INDIA' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n2_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation1 n1,
                        nation2 n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'ASIA'
                        and s_nationkey = n2.n2_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        s_name,
        count(*) as numwait
from
        supplier,
        lineitem l1,
        orders,
        nation
where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem2 l2
                where
                        l2.l2_orderkey = l1.l_orderkey
                        and l2.l2_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'ARGENTINA'
group by
        s_name
order by
        numwait desc,
        s_name;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT





 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        and ps_availqty > (select sum(c_nationkey) from customer where c_phone LIKE '%78-1123%')
order by
        s_name;
 --- extracted query:
  
 Select s_name, s_address 
 From customer, nation, part, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and customer.c_nationkey < partsupp.ps_availqty
 and nation.n_name = 'FRANCE'
 and customer.c_phone LIKE '%78-1123%'
 and part.p_name LIKE '%ivory%' 
 Order By s_name asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        and ps_availqty > (select sum(c_nationkey) from customer where n_nationkey = c_nationkey and c_phone LIKE '%78-1123%')
order by
        s_name;
 --- extracted query:
 Cannot do database minimization. Some problem in Regular mutation pipeline. Aborting extraction!
 From customer, nation, part, partsupp, supplier
 Where nation.n_nationkey = supplier.s_nationkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and nation.n_name = 'FRANCE'
 and customer.c_phone LIKE '%8-1123%'
 and customer.c_acctbal <= 3093.99
 and part.p_name LIKE '%ivory%'
 and partsupp.ps_availqty >= 2200;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
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
        and ps_availqty > (select sum(c_nationkey) from customer where c_phone LIKE '%78-1123%')
order by
        s_name;
 --- extracted query:
  
 Select s_name, s_address 
 From customer, nation, part, partsupp, supplier 
 Where nation.n_nationkey = supplier.s_nationkey
 and part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_suppkey = supplier.s_suppkey
 and customer.c_nationkey < partsupp.ps_availqty
 and nation.n_name = 'FRANCE'
 and customer.c_phone LIKE '%78-1123%'
 and part.p_name LIKE '%ivory%' 
 Order By s_name asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '3' month
        and exists (
                (select
                        *
                from
                        web_lineitem
                where
                        wl_orderkey = o_orderkey
                        and wl_commitdate < wl_receiptdate)
                UNION ALL
                (select
                        *
                from
                        store_lineitem
                where
                        sl_orderkey = o_orderkey
                        and sl_commitdate < sl_receiptdate)
        )
group by
        o_orderpriority
order by
        o_orderpriority;
 --- extracted query:
 too many values to unpack (expected 3)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '3' month
        and exists (
                (select
                        *
                from
                        web_lineitem
                where
                        wl_orderkey = o_orderkey
                        and wl_commitdate < wl_receiptdate)
                UNION ALL
                (select
                        *
                from
                        store_lineitem
                where
                        sl_orderkey = o_orderkey
                        and sl_commitdate < sl_receiptdate)
        )
group by
        o_orderpriority
order by
        o_orderpriority;
 --- extracted query:
 too many values to unpack (expected 3)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select
        sum(lineitem.l_extendedprice * lineitem.l_discount) as revenue
from
        (select wl_extendedprice as l_extendedprice,
        wl_discount as l_discount
        from web_lineitem 
        where wl_shipdate >= date '1993-01-01'
        and wl_shipdate < date '1994-03-01' + interval '1' year
        and wl_discount between 0.06 - 0.01 and 0.06 + 0.01
        and wl_quantity < 10
        UNION ALL
        select sl_extendedprice as l_extendedprice,
        sl_discount as l_discount
        from store_lineitem 
        where sl_shipdate >= date '1993-01-01'
        and sl_shipdate < date '1994-03-01' + interval '1' year
        and sl_discount between 0.06 - 0.01 and 0.06 + 0.01
        and sl_quantity < 10) as lineitem;
 --- extracted query:
 Some problem in Regular mutation pipeline. Aborting extraction!current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT
