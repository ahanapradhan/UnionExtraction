
 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 Select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice *(1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= date '1998-12-01' - interval '71 days' Group By l_returnflag, l_linestatus Order by l_returnflag, l_linestatus;

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey as orderkey, sum(l_discount*(1 + l_tax)) as revenue, o_totalprice as totalprice, o_shippriority as shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_totalprice, o_shippriority order by revenue desc, totalprice limit 10;
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey as orderkey, sum(l_discount*(1 + l_tax)) as revenue, o_totalprice as totalprice, o_shippriority as shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_totalprice, o_shippriority order by revenue desc, totalprice limit 10;
 --- extracted query:
 Some problem in filter extraction!Some problem in Regular mutation pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 30000000 
                         and l_quantity <= 123 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 Singular matrix
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 30000000 
                         and l_quantity <= 123 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 30000000 
                         and l_quantity <= 123 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 30000000 
                         and l_quantity <= 123 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey as orderkey, sum(l_discount*(1 + l_tax)) as revenue, o_totalprice as totalprice, o_shippriority as shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_totalprice, o_shippriority order by revenue desc, totalprice limit 10;
 --- extracted query:
  
 Select l_orderkey as orderkey, Sum(l_discount*(l_tax + 1)) as revenue, o_totalprice as totalprice, o_shippriority as shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and customer.c_mktsegment = 'BUILDING'
 and lineitem.l_shipdate >= '1995-03-16'
 and orders.o_orderdate <= '1995-03-14' 
 Group By l_orderkey, o_shippriority, o_totalprice 
 Order By revenue desc, totalprice asc, orderkey asc, shippriority asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority  from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= '1995-03-15' and l_shipdate >= '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;
 --- extracted query:
  
 Select l_orderkey, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and customer.c_mktsegment = 'BUILDING'
 and lineitem.l_shipdate >= '1995-03-15'
 and orders.o_orderdate <= '1995-03-15' 
 Group By l_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_tax <= 0.05 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_tax <= 0.05
 and orders.o_orderdate <= '1995-10-13' 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and l_extendedprice between 212 and 3000 group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_extendedprice between 212.00 and 3000.00
 and orders.o_orderdate <= '1995-10-13' 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 123
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_extendedprice between 212.00 and 3000.00
 and orders.o_orderdate <= '1995-10-13' 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and lineitem.l_extendedprice between 212.00 and 3000.00
 and orders.o_orderdate <= '1995-10-13' 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 800 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 800.0
 and lineitem.l_extendedprice between 212.00 and 3000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 500 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 600 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 800 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 800 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 900 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 900.0
 and lineitem.l_extendedprice between 212.00 and 3000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 850 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 850.0
 and lineitem.l_extendedprice between 212.00 and 3000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
 too many values to unpack (expected 2)
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority  from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= '1995-03-15' and l_shipdate >= '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;
 --- extracted query:
 current transaction is aborted, commands ignored until end of transaction block

 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority  from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= '1995-03-15' and l_shipdate >= '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;
 --- extracted query:
  
 Select l_orderkey, Sum(-l_discount*l_extendedprice + l_extendedprice + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and customer.c_mktsegment = 'BUILDING'
 and lineitem.l_shipdate >= '1995-03-15'
 and orders.o_orderdate <= '1995-03-15' 
 Group By l_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 700 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(-l_discount*l_extendedprice + l_extendedprice + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 700.0
 and lineitem.l_extendedprice between 212.00 and 3000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(-l_discount*l_extendedprice + l_extendedprice + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(-l_discount*l_extendedprice + l_extendedprice + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name, (2.24c_acctbal + 5.48o_totalprice + 2.5*o_shippriority + 325) as max_balance, o_clerk
FROM customer, orders
where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
and o_orderdate <= DATE '1995-10-23' and
c_acctbal > 30.04;
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 SELECT c_name, (2.24*c_acctbal + 5.48*o_totalprice + 2.5*o_shippriority + 325) as max_balance, o_clerk
FROM customer, orders
where c_custkey = o_custkey and o_orderdate > DATE '1993-10-14'
and o_orderdate <= DATE '1995-10-23' and
c_acctbal > 30.04;
 --- extracted query:
  
 Select c_name, 2.24*c_acctbal + 2.5*o_shippriority + 5.48*o_totalprice + 325 as max_balance, o_clerk 
 From customer, orders 
 Where customer.c_custkey = orders.o_custkey
 and customer.c_acctbal >= 30.05
 and orders.o_orderdate between '1993-10-15' and '1995-10-23';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority  from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= '1995-03-15' and l_shipdate >= '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;
 --- extracted query:
  
 Select l_orderkey, Sum(-l_discount*l_extendedprice + l_extendedprice + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and customer.c_mktsegment = 'BUILDING'
 and lineitem.l_shipdate >= '1995-03-15'
 and orders.o_orderdate <= '1995-03-15' 
 Group By l_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select l_orderkey, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority  from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate <= '1995-03-15' and l_shipdate >= '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;
 --- extracted query:
  
 Select l_orderkey, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and customer.c_mktsegment = 'BUILDING'
 and lineitem.l_shipdate >= '1995-03-15'
 and orders.o_orderdate <= '1995-03-15' 
 Group By l_orderkey, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, l_orderkey asc, o_shippriority asc 
 Limit 10;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select c_mktsegment, 
                         sum(l_extendedprice*(1-l_discount) + l_quantity) as revenue,
                         o_orderdate, o_shippriority 
                         from customer, orders, lineitem 
                         where c_custkey = o_custkey and l_orderkey = o_orderkey and 
                         o_orderdate <= date '1995-10-13' and 
                         l_extendedprice between 212 and 3000000 and l_quantity <= 123 
                         group by o_orderdate, o_shippriority, c_mktsegment 
                         order by revenue desc, o_orderdate asc, o_shippriority asc;
 --- extracted query:
  
 Select c_mktsegment, Sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, o_orderdate, o_shippriority 
 From customer, lineitem, orders 
 Where customer.c_custkey = orders.o_custkey
 and lineitem.l_orderkey = orders.o_orderkey
 and orders.o_orderdate <= '1995-10-13'
 and lineitem.l_quantity <= 123.0
 and lineitem.l_extendedprice between 212.00 and 3000000.00 
 Group By c_mktsegment, o_orderdate, o_shippriority 
 Order By revenue desc, o_orderdate asc, o_shippriority asc, c_mktsegment asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_nationkey IN (1, 2, 3, 5, 4, 10) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_nationkey IN (1, 2, 3, 5, 4, 10) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
 Cannot do database minimization. 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_nationkey IN (1, 2, 3, 5, 4, 10) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_acctbal between 1000.01 and 6999.99 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_nationkey > 3 and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_acctbal between 1000.01 and 6999.99
 and customer.c_nationkey >= 4 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_nationkey in (1,3) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_acctbal between 1000.01 and 6999.99 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_regionkey in (1,4,3) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and customer.c_acctbal between 1000.01 and 6999.99
 and nation.n_regionkey between 1 and 4 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_regionkey in (1,4) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
 Cannot do database minimization. Some problem in disjunction pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_regionkey in (1,4) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
 Cannot do database minimization. Some problem in disjunction pipeline. Aborting extraction!
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_regionkey in (2,4) and c_acctbal < 7000 and c_acctbal > 1000 ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey IN (2, 4)
 and customer.c_acctbal between 1000.01 and 6999.99 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, c_acctbal from nation, customer WHERE n_nationkey = c_nationkey and n_regionkey in (2,4) and ((c_acctbal < 7000 and c_acctbal > 1000) or (c_acctbal > 8500)) ORDER BY c_acctbal;
 --- extracted query:
  
 Select n_name, c_acctbal 
 From customer, nation 
 Where customer.c_nationkey = nation.n_nationkey
 and nation.n_regionkey IN (2, 4)
 and customer.c_acctbal >= 1000.01 
 Order By c_acctbal asc;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_brand, p_type from part, partsupp WHERE ps_partkey = p_partkey and p_size in (1,4) and ps_supplycost < 1000;
 --- extracted query:
  
 Select p_brand, p_type 
 From part, partsupp 
 Where part.p_partkey = partsupp.ps_partkey
 and part.p_size IN (1, 4)
 and partsupp.ps_supplycost <= 999.99;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_brand, p_type from part, partsupp WHERE ps_partkey = p_partkey and AND p_brand <> 'Brand#23' and ps_supplycost < 1000;
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_brand, p_type from part, partsupp WHERE ps_partkey = p_partkey  AND p_brand <> 'Brand#23' and ps_supplycost < 1000;
 --- extracted query:
  
 Select p_brand, p_type 
 From part, partsupp 
 Where part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_supplycost <= 999.99;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_brand, p_type from part, partsupp WHERE ps_partkey = p_partkey  AND p_brand <> 'Brand#23' and ps_supplycost < 1000;
 --- extracted query:
  
 Select p_brand, p_type 
 From part, partsupp 
 Where part.p_partkey = partsupp.ps_partkey
 and partsupp.ps_supplycost <= 999.99;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, r_name from nation, region WHERE n_regionkey = r_regionkey  AND n_name <> 'BRAZIL';
 --- extracted query:
  
 Select n_name, r_name 
 From nation, region 
 Where nation.n_regionkey = region.r_regionkey;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, r_name from nation, region WHERE n_regionkey = r_regionkey  AND n_name <> 'BRAZIL';
 --- extracted query:
  
 Select n_name, r_name 
 From nation, region 
 Where nation.n_regionkey = region.r_regionkey;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, r_name from nation, region WHERE n_regionkey = r_regionkey  AND n_name <> 'BRAZIL';
 --- extracted query:
  
 Select n_name, r_name 
 From nation, region 
 Where nation.n_regionkey = region.r_regionkey;
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, r_name from nation, region WHERE n_regionkey = r_regionkey  AND n_name <> 'BRAZIL';
 --- extracted query:
 local variable 'index' referenced before assignment
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select n_name, r_name from nation, region WHERE n_regionkey = r_regionkey  AND n_name <> 'BRAZIL';
 --- extracted query:
  
 Select n_name, r_name 
 From nation, region 
 Where nation.n_regionkey = region.r_regionkey
 and nation.n_name <> 'BRAZIL';
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_size from part where p_size in (1, 4, 7);
 --- extracted query:
  
 Select p_size 
 From part 
 Where part.p_size IN (1, 4, 7);
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_partkey,  from part where p_size in (1, 4, 7) and p_brand <> 'Brand#23' and 
        p_type NOT LIKE 'MEDIUM POLISHED%';
 --- extracted query:
 --- Extraction Failed! Nothing to show! 
 --- END OF ONE EXTRACTION EXPERIMENT

 --- START OF ONE EXTRACTION EXPERIMENT
 --- input query:
 select p_partkey  from part where p_size in (1, 4, 7) and p_brand <> 'Brand#23' and 
        p_type NOT LIKE 'MEDIUM POLISHED%';
 --- extracted query:
  
 Select p_partkey 
 From part 
 Where part.p_size IN (1, 4, 7)
 and part.p_brand <> 'Brand#23'
 and part.p_type NOT LIKE 'MEDIUM POLISHED%';
 --- END OF ONE EXTRACTION EXPERIMENT
