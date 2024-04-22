Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, Sum(l_extendedprice*(1 - l_discount)) as sum_disc_price, Sum(l_extendedprice*(-l_discount*l_tax - l_discount + l_tax + 1)) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, Avg(l_discount) as avg_disc, Count(*) as count_order
 From lineitem 
 Where l_shipdate <= l_receiptdate
 and l_receiptdate <= l_commitdate 
 Group By l_returnflag, l_linestatus 
 Order By l_returnflag asc, l_linestatus asc;