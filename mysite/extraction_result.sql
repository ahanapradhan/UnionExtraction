
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
